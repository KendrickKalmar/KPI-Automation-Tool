import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import time
from typing import Dict, Any
import config
import config_placement
import data_processing
import database
import parallel_executor
import sheet_placement
import query_config
import debug_utils  # Новый импорт
from data_processing import process_common_kpi, extract_single_value, process_statuses_data
from sheet_placement import update_sheet_precise

def setup_logging():
    """Настройка логирования"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler = RotatingFileHandler(config.LOG_FILE, maxBytes=config.LOG_MAX_BYTES, backupCount=config.LOG_BACKUP_COUNT)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

logger = setup_logging()

def log_execution_time(func):
    """Декоратор для логирования времени выполнения"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Начало выполнения {func.__name__}")
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"Завершение {func.__name__}. Время выполнения: {end_time - start_time:.2f} секунд")
        return result
    return wrapper

@log_execution_time
def get_week_dates() -> Dict[str, Any]:
    """Получение дат недели для обработки"""
    if config.DEBUG_MODE:
        return debug_utils.get_test_dates()
    
    today = datetime.now()
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + timedelta(days=6)
    end_week_exclusive = last_sunday + timedelta(days=1)
        
        dates = {
            'start_week': last_monday.strftime('%Y-%m-%d'),
            'end_week': last_sunday.strftime('%Y-%m-%d'),
            'end_week_exclusive': end_week_exclusive.strftime('%Y-%m-%d'),
            'daily_dates': [(last_monday + timedelta(days=i)).strftime('%Y-%m-%d') 
                           for i in range(7)],
            'week_number': last_monday.isocalendar()[1],
            'year': last_monday.year,
            'is_test_mode': False
        }
        
        logger.info(f"Даты обработки: {dates['start_week']} - {dates['end_week']}")
        logger.info(f"Исключающая дата для SQL: {dates['end_week_exclusive']}")
        logger.info(f"Неделя №{dates['week_number']}, {dates['year']} год")
        
        return dates

@log_execution_time
def get_google_sheet_client():
    """Создание клиента для работы с Google Sheets"""
    try:
        logger.info("Подключаемся к Google Sheets API")
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(config.SERVICE_ACCOUNT_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        logger.info("Успешное подключение к Google Sheets")
        return client
    except Exception as e:
        logger.error(f"Ошибка подключения к Google Sheets: {e}")
        return None

@log_execution_time
def run_custody_partner_deposits(engine, dates):
    """Расчет среднего времени CUSTODY PARTNER DEPOSITS за период"""
    if config.DEBUG_MODE and config.DEBUG_SETTINGS['skip_slow_queries']:
        logger.info("Пропускаем CUSTODY DEPOSITS в тестовом режиме")
        return pd.DataFrame()
    
    logger.info("Начинаем обработку CUSTODY PARTNER DEPOSITS AVG TIME")
    daily_results = []
    
    for i, day in enumerate(dates['daily_dates']):
        logger.info(f"Обрабатываем день {i+1}/{len(dates['daily_dates'])}: {day}")
        next_day = (datetime.strptime(day, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        
        query = """
            select date(registered_at), avg(duration_sending_at) as avg_duration_sending, count(*) from (
                SELECT distinct o.external_id, s.registered_at, es2.sending_at, s.registered_at - es2.sending_at as duration_sending_at
                FROM fincore.splits s
                  INNER JOIN fincore.transactions t ON t.id = s.transaction_id
                  INNER JOIN fincore.operations o ON o.id = t.operation_id
                  INNER JOIN LATERAL (
                    SELECT e.id, to_timestamp(((sc->>'changedAt')::jsonb ->> '$date')::bigint / 1000) AS sending_at FROM exchanger.exchanges_cashed e, 
                    json_array_elements(array_to_json(e.status_change)) sc 
                    WHERE e.id = o.external_id AND sc->>'to' = 'sending'
                    and e.created_at between :day and :next_day) es2 ON es2.id = o.external_id
                  INNER JOIN exchanger.exchanges_cashed e1 ON e1.id = o.external_id
                WHERE s.account_id IN (SELECT id FROM fincore.accounts pa WHERE "account_type" = 'LIABILITY')
                  AND o."operation_type" = 'EXCHANGE' and t.external_id like 'PO:c%'
                  AND e1.payout::text like '%changenow_partner%' 
                  and t.created_at between :day and :next_day
                  and o.created_at between :day and :next_day
                  and s.created_at between :day and :next_day
                 and lower(t.external_id) not like '%fee%'
            ) subq1 
            group by 1;
        """
        
        df_day = database.execute_query(engine, query, {'day': day, 'next_day': next_day}, 
                              f"CUSTODY_PARTNER_DEPOSITS_{day}")
        if not df_day.empty:
            daily_results.append(df_day)
            logger.info(f"Данные за {day}: avg_duration_sending={df_day['avg_duration_sending'].iloc[0]}, count={df_day['count'].iloc[0]}")
        else:
            logger.warning(f"Нет данных за {day}")
    
    # Вычисляем среднее значение за период
    if daily_results:
        total_seconds = 0
        total_count = 0
        
        for df in daily_results:
            count_val = df['count'].iloc[0]
            avg_seconds = df['avg_duration_sending'].iloc[0].total_seconds()
            total_seconds += avg_seconds * count_val
            total_count += count_val
        
        weighted_avg_seconds = total_seconds / total_count
        weighted_avg = pd.Timedelta(seconds=weighted_avg_seconds)
        
        result_df = pd.DataFrame({
            'metric': ['avg_duration_sending_period', 'total_count_period'],
            'value': [weighted_avg, total_count]
        })
        
        logger.info(f"Рассчитано средневзвешенное значение: {weighted_avg}, общее количество: {total_count}")
        return result_df
    
    logger.warning("Нет данных за весь период")
    return pd.DataFrame()

@log_execution_time
def main():
    """Основная функция"""
    logger.info("Запуск процесса обновления KPI")
    
    # Настройка тестового окружения
    if config.DEBUG_MODE:
        debug_utils.setup_test_environment()
    
    try:
        # Получаем даты
        dates = get_week_dates()
        
        # Подключаемся к БД
        engine = database.create_db_connection()
        if not engine:
            logger.error("Не удалось подключиться к БД. Процесс остановлен.")
            return
        
        # Подключаемся к Google Sheets
        client = get_google_sheet_client()
        if not client:
            logger.error("Не удалось подключиться к Google Sheets. Процесс остановлен.")
            return
        
        queries_to_execute = {}
        for query_name in query_config.PARALLEL_QUERIES:
            if query_name in query_config.SQL_QUERIES:
                query_config_data = query_config.SQL_QUERIES[query_name]
                query = query_config_data['query']
                params = {}
                for param_name, param_value in query_config_data['params'].items():
                    if param_value == ':start_week':
                        params[param_name] = dates['start_week']
                    elif param_value == ':end_week':
                        params[param_name] = dates['end_week']
                    elif param_value == ':end_week_exclusive':
                        params[param_name] = dates['end_week_exclusive']
                    else:
                        params[param_name] = param_value
                queries_to_execute[query_name] = (query, params)
        
        if config.DEBUG_MODE:
            queries_to_execute = debug_utils.get_test_queries(queries_to_execute)
        
        results = parallel_executor.execute_parallel_queries(engine, queries_to_execute, config.MAX_WORKERS)
        
        if not (config.DEBUG_MODE and config.DEBUG_SETTINGS['skip_slow_queries'] and 'custody_deposits' in queries_to_execute):
            results['custody_deposits'] = run_custody_partner_deposits(engine, dates)
        if 'common_kpi' in results:
            update_sheet_precise(client, results['common_kpi'], 'common_kpi', 'common_kpi')
        
        if 'custody_deposits' in results and not results['custody_deposits'].empty:
            avg_time_row = results['custody_deposits'][results['custody_deposits']['metric'] == 'avg_duration_sending_period']
            if not avg_time_row.empty:
                avg_time = avg_time_row['value'].iloc[0]
                update_sheet_precise(client, None, 'custody_deposits_avg', 'avg_duration_sending_weekly', avg_time)
        
        if 'tf_sr_payouts' in results and not results['tf_sr_payouts'].empty:
            for column in ['total_payouts', 'long_payouts']:
                if column in results['tf_sr_payouts'].columns:
                    value = extract_single_value(results['tf_sr_payouts'], column)
                    update_sheet_precise(client, None, 'tf_sr_payouts', column, value)

        if 'tf_sr_partner_liability' in results and not results['tf_sr_partner_liability'].empty:
            for column in ['total_payouts', 'long_payouts']:
                if column in results['tf_sr_partner_liability'].columns:
                    value = extract_single_value(results['tf_sr_partner_liability'], column)
                    update_sheet_precise(client, None, 'tf_sr_partner_liability', column, value)

        if 'tf_sr_median_payouts' in results and not results['tf_sr_median_payouts'].empty:
            value = extract_single_value(results['tf_sr_median_payouts'], 'median_duration')
            update_sheet_precise(client, None, 'tf_sr_median_payouts', 'median_duration', value)

        if 'tf_sr_submitted_to_finished' in results and not results['tf_sr_submitted_to_finished'].empty:
            value = extract_single_value(results['tf_sr_submitted_to_finished'], 'avg_duration')
            update_sheet_precise(client, None, 'tf_sr_submitted_to_finished', 'avg_duration', value)
        
        if 'statuses_payout_reward' in results and not results['statuses_payout_reward'].empty:
            update_sheet_precise(client, results['statuses_payout_reward'], 'statuses_payout_reward', None)

        if 'sr_payouts_slow' in results and not results['sr_payouts_slow'].empty:
            value = extract_single_value(results['sr_payouts_slow'], 'long_count')
            update_sheet_precise(client, None, 'sr_payouts_slow', 'long_count', value)

        if 'currency_stats' in results and not results['currency_stats'].empty:
            update_sheet_precise(client, results['currency_stats'], 'currency_stats', None)

        if 'statuses_partner_liability' in results and not results['statuses_partner_liability'].empty:
            update_sheet_precise(client, results['statuses_partner_liability'], 'statuses_partner_liability', None)

        update_sheet_precise(client, None, 'tf_sr_links_increment', None)
        
        logger.info("Процесс обновления KPI завершен успешно")
        
        if config.DEBUG_MODE:
            logger.info(f"Обработан период: {dates['start_week']} - {dates['end_week']}")
            logger.info(f"Выполнено запросов: {len(results)}")
            for query_name, result in results.items():
                status = "Успех" if not result.empty else "Пустой результат"
                logger.info(f"  {query_name}: {status} ({len(result)} строк)")
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        logger.exception("Детали ошибки:")

if __name__ == "__main__":
    try:
        main()
    finally:
        logging.shutdown()
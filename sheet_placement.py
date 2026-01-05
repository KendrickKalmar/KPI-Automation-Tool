import gspread
from google.oauth2.service_account import Credentials
import logging
import config
import config_placement
import pandas as pd
from datetime import datetime
from data_processing import process_common_kpi
from data_processing import parse_common_kpi_result
from data_processing import format_duration

logger = logging.getLogger(__name__)

def update_sheet_with_dates(client, df, config_key, data_name, value=None, dates=None):
    """Обновление данных с добавлением информации о датах"""
    try:
        if dates:
            logger.info(f"Обновление данных за период: {dates['start_week']} - {dates['end_week']}")
        
        # Основная логика размещения данных остается без изменений
        return update_sheet_precise(client, df, config_key, data_name, value)
    except Exception as e:
        logger.error(f"Ошибка при обновлении данных с датами: {e}")
        return False

def create_backup_entry(client, query_name, df, dates):
    """Создает резервную запись с данными и метаинформацией"""
    try:
        if query_name == 'common_kpi':
            parsed_data = parse_common_kpi_result(df)
            
            # Добавляем метаинформацию о периоде
            backup_df = pd.DataFrame({
                'query_name': [query_name],
                'period_start': [dates['start_week']],
                'period_end': [dates['end_week']],
                'processed_at': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                'data': [str(parsed_data)]
            })
            
            update_sheet_precise(client, backup_df, 'backup_common_kpi', None)
            logger.info("Резервная копия common_kpi создана")
        
        return True
    except Exception as e:
        logger.error(f"Ошибка создания резервной копии: {e}")
        return False

def get_spreadsheet_id(config_key):
    """Получаем реальный ID таблицы по ключу из конфига"""
    # Тестовая маршрутизация при DEBUG
    try:
        if getattr(config, 'DEBUG_MODE', False) and config.DEBUG_SETTINGS.get('use_test_spreadsheet'):
            test_map = getattr(config, 'SPREADSHEET_IDS_TEST', {})
            if config_key in test_map:
                return test_map[config_key]
    except Exception:
        pass
    if config_key in config.SPREADSHEET_IDS:
        return config.SPREADSHEET_IDS[config_key]
    return config_key  # Если это уже прямой ID

def format_timedelta_for_sheets(timedelta_value):
    """Форматирует timedelta для Google Sheets: 0 days 00:01:42.033043 -> 00:01:42,033043"""
    if pd.isna(timedelta_value) or timedelta_value is None:
        return ""
    
    # Преобразуем в строку
    time_str = str(timedelta_value)
    
    # Убираем "0 days " если есть
    if time_str.startswith("0 days "):
        time_str = time_str[7:]  # Убираем "0 days "
    
    # Заменяем точку на запятую в миллисекундах
    if '.' in time_str:
        time_str = time_str.replace('.', ',')
    
    return time_str

def update_sheet_precise(client, df, config_key, data_name, value=None):
    """Размещение данных в Google Sheets согласно конфигурации"""
    logger.info(f"update_sheet_precise: config_key={config_key}, data_name={data_name}, df is not None={df is not None}, value={value}")
    try:
        if config_key not in config_placement.PLACEMENT_CONFIG:
            logger.error(f"Конфигурация для {config_key} не найдена")
            return False
        
        config_data = config_placement.PLACEMENT_CONFIG[config_key]
        spreadsheet_id = get_spreadsheet_id(config_data['spreadsheet_id'])
        sheet_name = config_data['sheet_name']
        data_type = config_data['data_type']
        
        logger.info(f"Открываем таблицу {spreadsheet_id}, лист {sheet_name}")
        
        try:
            # Получаем таблицу и лист
            spreadsheet = client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Лист '{sheet_name}' не найден в таблице {spreadsheet_id}")
            # Попробуем получить список всех листов для отладки
            try:
                all_sheets = [ws.title for ws in spreadsheet.worksheets()]
                logger.info(f"Доступные листы: {all_sheets}")
            except:
                pass
            return False
        except gspread.exceptions.APIError as e:
            logger.error(f"Ошибка доступа к таблице {spreadsheet_id}: {e}")
            return False
        
        # Единоразово записываем диапазон дат недели в первую ячейку новой строки для листов TF_SR и Services
        try:
            if sheet_name in ('TF_SR', 'Services'):
                if not hasattr(update_sheet_precise, '_date_written_cache'):
                    update_sheet_precise._date_written_cache = {}
                cache_key = f"{worksheet.spreadsheet.id}:{worksheet.title}"
                if not update_sheet_precise._date_written_cache.get(cache_key):
                    # Определяем следующую свободную строку
                    base_row_hint = 121 if sheet_name == 'Services' else 74 if sheet_name == 'TF_SR' else 2
                    current_row = len(worksheet.get_all_values()) + 1
                    current_row = max(current_row, base_row_hint)
                    date_cell = f"A{current_row}"

                    # Формируем человекочитаемый диапазон дат (Пн-Вс)
                    from debug_utils import format_week_date_range
                    from datetime import datetime, timedelta
                    start_date = config.DEBUG_SETTINGS.get('test_date')
                    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                    # Вс включительно — +6 дней
                    end_dt = start_dt + timedelta(days=6)
                    date_range = format_week_date_range(start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d'))

                    logger.info(f"Записываем диапазон дат недели в {sheet_name}!{date_cell}: '{date_range}'")
                    if not config.SHEETS_DEBUG.get('dry_run'):
                        worksheet.update(date_cell, [[date_range]], value_input_option='RAW')
                        if config.SHEETS_DEBUG.get('verify_writes'):
                            back = worksheet.acell(date_cell).value
                            logger.info(f"verify {date_cell}: '{back}'")
                    # Зафиксируем текущую целевую строку в row_cache, чтобы все значения этой недели попали в неё
                    if not hasattr(update_sheet_precise, '_row_cache'):
                        update_sheet_precise._row_cache = {}
                    key = f"{worksheet.spreadsheet.id}:{worksheet.title}"
                    update_sheet_precise._row_cache[key] = current_row
                    update_sheet_precise._date_written_cache[cache_key] = True
        except Exception as e:
            logger.error(f"Ошибка записи даты недели: {e}")
        
        # ... остальной код функции
        
        # Вспомогательные функции
        # Кэширование вычисленного next_row на время вызова (чтобы все записи встали в одну строку)
        if not hasattr(update_sheet_precise, '_row_cache'):
            update_sheet_precise._row_cache = {}
        def cache_key(worksheet):
            try:
                return f"{worksheet.spreadsheet.id}:{worksheet.title}"
            except Exception:
                return worksheet.title
        def find_next_row_for_sheet(worksheet, base_row_hint: int) -> int:
            key = cache_key(worksheet)
            if key in update_sheet_precise._row_cache:
                return update_sheet_precise._row_cache[key]
            try:
                last_row = len(worksheet.get_all_values())
                next_row = max(last_row + 1, base_row_hint)
            except Exception:
                next_row = base_row_hint
            update_sheet_precise._row_cache[key] = next_row
            return next_row

        def increment_formula_row(cell_value: str) -> str:
            # Примитивный инкремент ссылки TF_SR!X73 -> TF_SR!X74
            try:
                if isinstance(cell_value, str) and 'TF_SR!' in cell_value:
                    import re
                    def repl(m):
                        return str(int(m.group(0)) + 1)
                    return re.sub(r"(?<=TF_SR![A-Z]{1,3})\d+", repl, cell_value)
            except Exception:
                pass
            return cell_value
        
        # Обработка разных типов вставки
        
        # Специальная обработка для E16 формулы (sr_payouts_slow)
        if config_key == 'sr_payouts_slow' and value is not None and data_name == 'long_count':
            # E16: формула =1-x%, где x — value (например, '52.78%')
            try:
                target_cell = 'E16'
                sval = str(value).strip()
                logger.info(f"E16: Исходное значение: '{sval}'")
                # Формируем формулу в правильном формате для Google Sheets
                if sval.endswith('%'):
                    # Убираем % и заменяем точку на запятую
                    num_with_comma = sval[:-1].strip().replace('.', ',')
                    logger.info(f"E16: Число с запятой: '{num_with_comma}'")
                    formula = f"=1-{num_with_comma}%"
                else:
                    # Если не процент, конвертируем в проценты
                    try:
                        num = float(sval.replace(',', '.'))
                        # Конвертируем в проценты и форматируем с запятой
                        percent_str = f"{num * 100:.2f}".replace('.', ',')
                        formula = f"=1-{percent_str}%"
                    except Exception:
                        formula = "=1-0%"
                logger.info(f"E16: Финальная формула: '{formula}'")
                if not config.SHEETS_DEBUG.get('dry_run'):
                    # Записываем формулу
                    try:
                        worksheet.update(target_cell, [[formula]], value_input_option='USER_ENTERED')
                        logger.info(f"Формула записана: {formula}")
                    except Exception as e:
                        logger.error(f"Ошибка записи формулы: {e}")
                    
                    if config.SHEETS_DEBUG.get('verify_writes'):
                        back = worksheet.acell(target_cell).value
                        logger.info(f"verify {target_cell}: '{back}'")
                logger.info(f"Обновлена формула в {target_cell}: {formula}")
                return True
            except Exception as e:
                logger.error(f"Ошибка обновления формулы E16: {e}")
        
        if data_type == 'single_value' and value is not None:
            cell_address = config_data['placement'][data_name]
            # Особые случаи до дефолта: авто-строки

            # Авто-строки для Services/TF_SR
            if config_key in ('tf_sr_payouts', 'tf_sr_partner_liability', 'tf_sr_submitted_to_finished', 'tf_sr_median_payouts', 'custody_deposits_avg') or sheet_name in ('Services', 'TF_SR'):
                try:
                    import re
                    col = re.sub(r"\d+", "", cell_address)
                    base_row_hint = 121 if sheet_name == 'Services' else 74 if sheet_name == 'TF_SR' else 2
                    next_row = find_next_row_for_sheet(worksheet, base_row_hint)
                    new_cell = f"{col}{next_row}"
                    # Форматируем timedelta для вставки в ячейку (и TF_SR, и Services)
                    if isinstance(value, pd.Timedelta) or ('days' in str(value)):
                        formatted_value = format_timedelta_for_sheets(value)
                        logger.info(f"format timedelta for cell: {value} -> {formatted_value}")
                    else:
                        formatted_value = str(value)
                    
                    if config.SHEETS_DEBUG.get('log_ranges'):
                        logger.info(f"single_value auto_row -> {new_cell} value={formatted_value}")
                    if not config.SHEETS_DEBUG.get('dry_run'):
                        worksheet.update(new_cell, [[formatted_value]])
                        if config.SHEETS_DEBUG.get('verify_writes'):
                            back = worksheet.acell(new_cell).value
                            logger.info(f"verify {new_cell}: '{back}'")
                    return True
                except Exception as e:
                    logger.error(f"Ошибка single_value auto_row для {config_key}: {e}")
                    # если авто-строка не сработала, падаем на дефолт ниже

            # Дефолт: запись в указанный адрес
            if config.SHEETS_DEBUG.get('log_ranges'):
                logger.info(f"single_value -> {cell_address} value={value}")
            if not config.SHEETS_DEBUG.get('dry_run'):
                worksheet.update(cell_address, [[str(value)]])
                if config.SHEETS_DEBUG.get('verify_writes'):
                    back = worksheet.acell(cell_address).value
                    logger.info(f"verify {cell_address}: '{back}'")
            return True
        
        elif data_type == 'range' and df is not None:
            # Вставка в диапазон ячеек
            if data_name in config_data['placement']:
                cell_address = config_data['placement'][data_name]
                if df is not None and not df.empty and data_name in df.columns:
                    value = df[data_name].iloc[0] if not df.empty else ''
                    if config.SHEETS_DEBUG.get('log_ranges'):
                        logger.info(f"range -> {cell_address} value={value}")
                    if not config.SHEETS_DEBUG.get('dry_run'):
                        worksheet.update(cell_address, [[str(value)]])
                        if config.SHEETS_DEBUG.get('verify_writes'):
                            back = worksheet.acell(cell_address).value
                            logger.info(f"verify {cell_address}: '{back}'")
                return True
            return False
        
        elif data_type == 'table' and not df.empty:
            # Вставка таблицы
            start_cell = config_data['placement']['start_cell']
            
            # Конвертируем timedelta в нужный формат
            df = df.copy()
            for col in df.columns:
                if df[col].dtype == 'timedelta64[ns]':
                    df[col] = df[col].apply(lambda x: format_duration(x) if hasattr(x, 'total_seconds') else x)

            # Очищаем старые данные
            if not config.SHEETS_DEBUG.get('dry_run'):
                worksheet.clear()
            
            # Вставляем заголовки
            headers = df.columns.tolist()
            if config.SHEETS_DEBUG.get('log_ranges'):
                logger.info(f"table headers -> {start_cell} cols={len(headers)}")
            if not config.SHEETS_DEBUG.get('dry_run'):
                worksheet.update(start_cell, [headers])
    
    # Вставляем данные
            values = df.values.tolist()
            if values:
                start_row = int(''.join(filter(str.isdigit, start_cell))) + 1  # +1 для данных после заголовков
                start_col = ''.join(filter(str.isalpha, start_cell))
                data_range = f"{start_col}{start_row}"
                if config.SHEETS_DEBUG.get('log_ranges'):
                    logger.info(f"table values -> {data_range} rows={len(values)} cols={len(values[0])}")
                if not config.SHEETS_DEBUG.get('dry_run'):
                    worksheet.update(data_range, values)
            
            logger.info(f"Вставлена таблица размером {len(values)}x{len(values[0])}")
            return True
        
        elif data_type == 'common_kpi_table' and not df.empty:
            # Специальная обработка для common_kpi с авто-строкой на листе Services
            processed_data = process_common_kpi(df)
            import re
            base_row_hint = 121 if sheet_name == 'Services' else 2
            
            # Каждая метрика получает свою строку
            current_row = base_row_hint
            for metric_name, metric_data in processed_data.items():
                if metric_name in config_data['placement']:
                    # Находим следующую свободную строку для этой метрики
                    next_row = find_next_row_for_sheet(worksheet, current_row)
                    current_row = next_row + 1  # Следующая метрика будет в следующей строке
                    
                    metric_config = config_data['placement'][metric_name]
                    for value_key, cell_address in metric_config.items():
                        value_index = int(value_key.replace('value', '')) - 1
                        if value_index < len(metric_data['values']):
                            value = metric_data['values'][value_index]
                            col = re.sub(r"\d+", "", cell_address)
                            new_cell = f"{col}{next_row}"
                            if config.SHEETS_DEBUG.get('log_ranges'):
                                logger.info(f"common_kpi auto_row -> {metric_name}.{value_key} {new_cell} value={value}")
                            if not config.SHEETS_DEBUG.get('dry_run'):
                                worksheet.update(new_cell, [[str(value)]])
                                if config.SHEETS_DEBUG.get('verify_writes'):
                                    back = worksheet.acell(new_cell).value
                                    logger.info(f"verify {new_cell}: '{back}'")
            # Не возвращаемся здесь, продолжаем для других операций

        # Специальные операции для листа status: сдвиги и формулы
        logger.info(f"DEBUG: config_key={config_key}, df is not None={df is not None}, data_name={data_name}")
        logger.info(f"DEBUG: config_key == 'statuses_payout_reward': {config_key == 'statuses_payout_reward'}")
        logger.info(f"DEBUG: config_key == 'statuses_partner_liability': {config_key == 'statuses_partner_liability'}")
        logger.info(f"DEBUG: df is not None: {df is not None}")
        logger.info(f"DEBUG: df type: {type(df)}")
        if df is not None:
            logger.info(f"DEBUG: df.empty: {df.empty if hasattr(df, 'empty') else 'no empty attr'}")
        logger.info(f"ПРОВЕРКА УСЛОВИЯ: config_key={config_key}, df is not None={df is not None}")
        logger.info(f"УСЛОВИЕ: config_key in ['statuses_payout_reward', 'statuses_partner_liability'] = {config_key in ['statuses_payout_reward', 'statuses_partner_liability']}")
        if config_key in ['statuses_payout_reward', 'statuses_partner_liability'] and df is not None:
            logger.info(f"ВХОДИМ В БЛОК STATUSES: {config_key}")
            logger.info(f"STATUS {config_key}: df.shape={df.shape}, columns={list(df.columns)}")
            logger.info(f"STATUS {config_key}: df.head(3)=\n{df.head(3)}")
            
            if config_key == 'statuses_payout_reward':
                # Сдвиг D4:D16 -> C4:C16, E4:E15 -> D4:D16, вставка E5:E15
                try:
                    # Диапазоны
                    range_c = 'C4:C16'
                    range_d = 'D4:D16'
                    range_e_4_15 = 'E4:E15'
                    range_e_5_15 = 'E5:E15'

                    # Читаем текущие значения столбцов D и E
                    values_d = worksheet.get(range_d)
                    values_e_4_15 = worksheet.get(range_e_4_15)

                    # Сдвиг: D -> C
                    if config.SHEETS_DEBUG.get('log_ranges'):
                        logger.info(f"shift D->C {range_d} -> {range_c}")
                    if values_d and not config.SHEETS_DEBUG.get('dry_run'):
                        worksheet.update(range_c, values_d)

                    # Сдвиг: E4:E15 -> D4:D16
                    if config.SHEETS_DEBUG.get('log_ranges'):
                        logger.info(f"shift E4:E15->D4:D16 {range_e_4_15} -> {range_d}")
                    if values_e_4_15 and not config.SHEETS_DEBUG.get('dry_run'):
                        worksheet.update(range_d, values_e_4_15)

                    # Вставляем новые значения в E5:E15 с форматированием времени
                    if not df.empty:
                        row = df.iloc[0]  # Берем первую строку
                        new_values = []
                        for col in df.columns:
                            val = row[col]
                            if pd.isna(val) or val is None:
                                new_values.append("")
                            elif isinstance(val, pd.Timedelta):
                                formatted_val = format_timedelta_for_sheets(val)
                                new_values.append(formatted_val)
                            else:
                                new_values.append(str(val))
                        
                        if config.SHEETS_DEBUG.get('log_ranges'):
                            logger.info(f"insert E5:E15 new_values={new_values}")
                        if not config.SHEETS_DEBUG.get('dry_run'):
                            # Правильный формат для Google Sheets: список списков
                            worksheet.update(range_e_5_15, [[val] for val in new_values], value_input_option='RAW')
                            if config.SHEETS_DEBUG.get('verify_writes'):
                                verify_values = worksheet.get(range_e_5_15)
                                logger.info(f"verify E5:E15: {verify_values}")

                    logger.info("Сдвиги и вставки statuses_payout_reward выполнены")
                    return True
                except Exception as e:
                    logger.error(f"Ошибка обработки statuses_payout_reward: {e}")
                    return False
            
            elif config_key == 'statuses_partner_liability':
                # Вставка значений в C20:C25
                try:
                    if not df.empty:
                        row = df.iloc[0]  # Берем первую строку
                        new_values = []
                        for col in df.columns:
                            val = row[col]
                            if pd.isna(val) or val is None:
                                new_values.append("")
                            elif isinstance(val, pd.Timedelta):
                                formatted_val = format_timedelta_for_sheets(val)
                                new_values.append(formatted_val)
                            else:
                                new_values.append(str(val))
                        
                        if config.SHEETS_DEBUG.get('log_ranges'):
                            logger.info(f"insert C20:C25 new_values={new_values}")
                        if not config.SHEETS_DEBUG.get('dry_run'):
                            worksheet.update('C20:C25', [[val] for val in new_values], value_input_option='RAW')
                            if config.SHEETS_DEBUG.get('verify_writes'):
                                verify_values = worksheet.get('C20:C25')
                                logger.info(f"verify C20:C25: {verify_values}")

                    logger.info("Вставки statuses_partner_liability выполнены")
                    return True
                except Exception as e:
                    logger.error(f"Ошибка обработки statuses_partner_liability: {e}")
                    return False

        # Специальные операции для листов Services и TF_SR: авто-определение следующей строки
        if config_key in ('common_kpi', 'custody_deposits_avg', 'tf_sr_payouts', 'tf_sr_partner_liability', 'tf_sr_submitted_to_finished', 'tf_sr_median_payouts'):
            # Базовые хинты: Services -> 121; TF_SR -> 74 (пользователь указал 73 уже занято)
            base_row_hint = 121 if sheet_name == 'Services' else 74 if sheet_name == 'TF_SR' else 2
            try:
                # Пересчитываем адрес ячейки, заменяя номер строки на next_row
                if data_name and data_name in config_data['placement']:
                    cell_address = config_data['placement'][data_name]
                    import re
                    col = re.sub(r"\d+", "", cell_address)
                    next_row = find_next_row_for_sheet(worksheet, base_row_hint)
                    new_cell = f"{col}{next_row}"
                    if value is not None:
                        # Форматируем время для TF_SR листа
                        logger.info(f"DEBUG TF_SR: sheet_name={sheet_name}, value={value}, type={type(value)}, str_value={str(value)}")
                        if sheet_name == 'TF_SR' and (isinstance(value, pd.Timedelta) or 'days' in str(value)):
                            formatted_value = format_timedelta_for_sheets(value)
                            logger.info(f"TF_SR форматирование времени: {value} -> {formatted_value}")
                        else:
                            formatted_value = str(value)
                        
                        if config.SHEETS_DEBUG.get('log_ranges'):
                            logger.info(f"auto_row -> {new_cell} value={formatted_value}")
                        if not config.SHEETS_DEBUG.get('dry_run'):
                            worksheet.update(new_cell, [[formatted_value]])
                            if config.SHEETS_DEBUG.get('verify_writes'):
                                back = worksheet.acell(new_cell).value
                                logger.info(f"verify {new_cell}: '{back}'")
                        # Не возвращаемся здесь, продолжаем для других операций
            except Exception as e:
                logger.error(f"Ошибка авто-строки для {config_key}: {e}")


        if config_key == 'tf_sr_links_increment':
            # Установка/инкремент ссылок и процентов в статусе:
            # J3:=ОКРУГЛ(100*TF_SR!G{row};2)&"%"
            # J4:=ОКРУГЛ(100*status!$F$16;2)&"%"
            # J5:=""&ОКРУГЛ(100*TF_SR!J{row};2)&"%"
            # J6:=ОКРУГЛ(100*TF_SR!M{row};2)&"%"
            # L3:=TF_SR!F{row}; L4:=status!$E$16; L5:=TF_SR!I{row}; L6:=TF_SR!L{row}
            try:
                logger.info("Начинаем инкремент ссылок TF_SR в I3/I5/I6")
                
                # Сначала найдем текущую строку TF_SR (последнюю заполненную)
                base_row_hint = 74 if sheet_name == 'TF_SR' else 74
                current_tf_sr_row = find_next_row_for_sheet(worksheet, base_row_hint) - 1
                next_tf_sr_row = current_tf_sr_row + 1
                
                logger.info(f"Текущая строка TF_SR: {current_tf_sr_row}, следующая: {next_tf_sr_row}")
                
                # Определяем формулы для J3:J6 и L3:L6 согласно требованиям локали RU (с ;)
                formulas = {
                    'J3': f'=ОКРУГЛ(100*TF_SR!G{next_tf_sr_row};2)&"%"',
                    'J4': '=ОКРУГЛ(100*status!$F$16;2)&"%"',
                    'J5': f'=""&ОКРУГЛ(100*TF_SR!J{next_tf_sr_row};2)&"%"',
                    'J6': f'=ОКРУГЛ(100*TF_SR!M{next_tf_sr_row};2)&"%"',
                    'L3': f'=TF_SR!F{next_tf_sr_row}',
                    'L4': '=status!$E$16',
                    'L5': f'=TF_SR!I{next_tf_sr_row}',
                    'L6': f'=TF_SR!L{next_tf_sr_row}'
                }
                
                for cell, formula in formulas.items():
                    logger.info(f"Устанавливаем формулу в {cell}: '{formula}'")
                    if not config.SHEETS_DEBUG.get('dry_run'):
                        try:
                            # USER_ENTERED гарантирует интерпретацию формулы без ведущей кавычки
                            worksheet.update(cell, [[formula]], value_input_option='USER_ENTERED')
                            if config.SHEETS_DEBUG.get('verify_writes'):
                                back = worksheet.acell(cell).value
                                logger.info(f"verify {cell}: '{back}'")
                        except Exception as e:
                            logger.error(f"Ошибка установки формулы в {cell}: {e}")
                            # Попробуем без value_input_option
                            try:
                                worksheet.update(cell, [[formula]], value_input_option='USER_ENTERED')
                                if config.SHEETS_DEBUG.get('verify_writes'):
                                    back = worksheet.acell(cell).value
                                    logger.info(f"verify {cell} (fallback): '{back}'")
                            except Exception as e2:
                                logger.error(f"Ошибка установки формулы в {cell} (fallback): {e2}")
                
                logger.info("Инкремент ссылок TF_SR в I3/I5/I6 выполнен")
                return True
            except Exception as e:
                logger.error(f"Ошибка инкремента ссылок TF_SR в I3/I5/I6: {e}")
        
        return False
    except Exception as e:
        logger.error(f"Ошибка точного размещения {config_key}.{data_name}: {e}")
        return False
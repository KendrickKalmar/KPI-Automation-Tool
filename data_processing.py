import pandas as pd
import re
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def parse_common_kpi_result(df):
    """Парсим результат common_kpi и преобразуем в структурированные данные"""
    if df.empty:
        return {}
    
    try:
        result_str = df.iloc[0, 0]
        pattern = r'\(([^)]+)\)'
        matches = re.findall(pattern, result_str)
        
        parsed_data = {}
        for match in matches:
            parts = [part.strip().strip('"') for part in match.split(',')]
            if len(parts) >= 6:
                metric_name = parts[2].strip()
                values = parts[3:]
                
                # Обрабатываем специальные случаи
                if metric_name == "custody_conversion ":
                    metric_name = "custody_conversion"
                
                parsed_data[metric_name] = {
                    'values': values,
                    'start_date': parts[0],
                    'end_date': parts[1]
                }
        
        return parsed_data
    except Exception as e:
        logger.error(f"Ошибка парсинга common_kpi: {e}")
        return {}

def format_date_range(start_date, end_date):
    """Форматирует диапазон дат для отображения"""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    return f"{start.strftime('%d.%m.%Y')} - {end.strftime('%d.%m.%Y')}"

def validate_date_range(start_date, end_date):
    """Проверяет корректность диапазона дат"""
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        if start > end:
            raise ValueError("Начальная дата не может быть больше конечной")
        
        if (end - start).days > 31:
            raise ValueError("Диапазон дат не может превышать 31 день")
        
        return True
    except ValueError as e:
        logger.error(f"Некорректный диапазон дат: {e}")
        return False

def process_common_kpi(df):
    """Обработка данных common_kpi - преобразуем результат в структурированный формат"""
    result = {}
    
    if not df.empty:
        try:
            # Обрабатываем все строки из результата
            for idx, row in df.iterrows():
                result_str = row.iloc[0]
                logger.info(f"process_common_kpi: row {idx}, result_str={result_str}")
            # Регулярное выражение для извлечения данных в скобках
            pattern = r'\(([^)]+)\)'
            matches = re.findall(pattern, result_str)
                logger.info(f"process_common_kpi: row {idx}, matches={matches}")
            
            for match in matches:
                parts = [part.strip().strip('"') for part in match.split(',')]
                    logger.info(f"process_common_kpi: row {idx}, parts={parts}")
                if len(parts) >= 6:
                    metric_name = parts[2]
                    values = parts[3:]  # Все значения начиная с 4-го элемента
                    
                    # Обрабатываем специальные случаи
                        if metric_name == "custody_conversion ":
                        metric_name = "custody_conversion"
                        elif metric_name == "reward ":
                            metric_name = "reward"
                    
                    result[metric_name] = {
                        'values': values,
                        'start_date': parts[0],
                        'end_date': parts[1]
                    }
                        logger.info(f"process_common_kpi: row {idx}, added {metric_name} with values {values}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки common_kpi: {e}")
    
    logger.info(f"process_common_kpi: final result={result}")
    return result

def format_duration(duration):
    """Форматирует длительность в читаемый вид HH:MM:SS.microseconds"""
    if hasattr(duration, 'total_seconds'):
        total_seconds = duration.total_seconds()
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        microseconds = int((total_seconds - int(total_seconds)) * 1_000_000)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{microseconds:06d}"
    return str(duration)

def process_statuses_data(df):
    """Обработка данных статусов для вставки в диапазон"""
    result = {}
    if not df.empty:
        for column in df.columns:
            if column.startswith('_'):
                value = df[column].iloc[0] if not df.empty else ''
                # Преобразуем timedelta в секунды (числовое значение для последующей вставки)
                if hasattr(value, 'total_seconds'):
                    result[column] = value.total_seconds()
                else:
                    result[column] = value
    return result

def extract_single_value(df, column_name):
    """Извлечение одиночного значения из DataFrame"""
    if not df.empty and column_name in df.columns:
        return df[column_name].iloc[0]
    return None

def convert_timedelta_to_seconds(value):
    """Преобразует timedelta в секунды (число)"""
    if hasattr(value, 'total_seconds'):
        return value.total_seconds()
    return value

# Примечание: значения статусов конвертируются в секунды для единообразного размещения
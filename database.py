import pandas as pd
from sqlalchemy import create_engine, text
import logging
import config

logger = logging.getLogger(__name__)

def create_db_connection():
    """Создание подключения к базе данных"""
    try:
        logger.info("Пытаемся подключиться к базе данных")
        connection_string = f"postgresql+psycopg2://{config.DB_CONFIG['user']}:{config.DB_CONFIG['password']}@{config.DB_CONFIG['host']}:{config.DB_CONFIG['port']}/{config.DB_CONFIG['database']}"
        engine = create_engine(connection_string)
        logger.info("Успешное подключение к БД")
        return engine
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {e}")
        return None

def execute_query(engine, query, params=None, query_name="Unknown"):
    """Выполнение SQL-запроса"""
    try:
        logger.info(f"Выполняем запрос: {query_name}")
        
        if params:
            df = pd.read_sql_query(text(query), engine, params=params)
        else:
            df = pd.read_sql_query(text(query), engine)
        
        logger.info(f"Запрос {query_name} выполнен. Получено {len(df)} строк")
        
        # Детальный вывод результатов для отладки
        if not df.empty:
            logger.info(f"Структура результата {query_name}:")
            logger.info(f"Колонки: {list(df.columns)}")
            logger.info(f"Первые 3 строки:\n{df.head(3).to_string(index=False)}")
            logger.info(f"Типы данных:\n{df.dtypes.to_string()}")
        else:
            logger.warning(f"Запрос {query_name} вернул пустой результат")
        
        return df
    except Exception as e:
        logger.error(f"Ошибка выполнения запроса {query_name}: {e}")
        return pd.DataFrame()
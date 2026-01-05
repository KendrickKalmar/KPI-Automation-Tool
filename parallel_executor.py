import concurrent.futures
from functools import partial
import logging
import database
import pandas as pd

logger = logging.getLogger(__name__)

def execute_parallel_queries(engine, queries_with_params, max_workers=5):
    """Выполнение запросов параллельно"""
    results = {}
    
    # Создаем partial функцию для выполнения запроса
    execute_func = partial(execute_query_with_name, engine)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Запускаем запросы параллельно
        future_to_query = {executor.submit(execute_func, query, params, name): name 
                          for name, (query, params) in queries_with_params.items()}
        
        # Собираем результаты
        for future in concurrent.futures.as_completed(future_to_query):
            query_name = future_to_query[future]
            try:
                results[query_name] = future.result()
                logger.info(f"Запрос {query_name} завершен")
            except Exception as e:
                logger.error(f"Ошибка в запросе {query_name}: {e}")
                results[query_name] = pd.DataFrame()
    
    return results

def execute_query_with_name(engine, query, params, name):
    """Вспомогательная функция для выполнения запроса с именем"""
    return database.execute_query(engine, query, params, name)
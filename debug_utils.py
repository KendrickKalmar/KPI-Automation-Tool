import pandas as pd
from datetime import datetime, timedelta
import logging
import config

logger = logging.getLogger(__name__)

def get_test_dates():
    """Получаем даты для тестового режима (1 день вместо недели)"""
    test_date = datetime.strptime(config.DEBUG_SETTINGS['test_date'], '%Y-%m-%d')
    test_end_date = test_date + timedelta(days=1)
    
    return {
        'start_week': test_date.strftime('%Y-%m-%d'),
        'end_week': test_date.strftime('%Y-%m-%d'),
        'end_week_exclusive': test_end_date.strftime('%Y-%m-%d'),
        'daily_dates': [test_date.strftime('%Y-%m-%d')],
        'week_number': test_date.isocalendar()[1],
        'year': test_date.year,
        'is_test_mode': True
    }

def get_test_queries(original_queries):
    """Фильтруем запросы для тестового режима"""
    if not config.DEBUG_SETTINGS['skip_slow_queries']:
        return original_queries
    
    # Исключаем медленные запросы из тестового режима
    slow_queries = ['custody_deposits', 'currency_stats', 'statuses_payout_reward', 'statuses_partner_liability']
    
    test_queries = {}
    for query_name, query_data in original_queries.items():
        if query_name not in slow_queries:
            test_queries[query_name] = query_data
    
    logger.info(f"Тестовый режим: выполняем только {len(test_queries)} из {len(original_queries)} запросов")
    return test_queries

def setup_test_environment():
    """Настройка тестового окружения"""
    if config.DEBUG_MODE:
        logger.info("=" * 50)
        logger.info("РЕЖИМ ОТЛАДКИ АКТИВИРОВАН")
        logger.info("=" * 50)
        logger.info("Настройки отладки:")
        for key, value in config.DEBUG_SETTINGS.items():
            logger.info(f"  {key}: {value}")
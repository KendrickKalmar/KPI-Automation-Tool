import os

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'your-db-host'),
    'port': os.getenv('DB_PORT', '5432'), 
    'database': os.getenv('DB_NAME', 'your-database'),
    'user': os.getenv('DB_USER', 'your-username'),
    'password': os.getenv('DB_PASSWORD', 'your-password')
}

# Google Sheets configuration
SPREADSHEET_IDS = {
    'sr_plus': os.getenv('SPREADSHEET_ID_SR_PLUS', 'your-spreadsheet-id-1'),
    'depo_kpi': os.getenv('SPREADSHEET_ID_DEPO_KPI', 'your-spreadsheet-id-2')
}

SPREADSHEET_IDS_TEST = {
    'sr_plus': os.getenv('TEST_SPREADSHEET_ID_SR_PLUS', SPREADSHEET_IDS['sr_plus']),
    'depo_kpi': os.getenv('TEST_SPREADSHEET_ID_DEPO_KPI', SPREADSHEET_IDS['depo_kpi'])
}

SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_CREDENTIALS_FILE', 'google_credentials.json')

# Parallel execution settings
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '3'))

# Debug mode
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'

DEBUG_SETTINGS = {
    'test_period_days': int(os.getenv('DEBUG_TEST_PERIOD_DAYS', '7')),
    'use_test_spreadsheet': os.getenv('DEBUG_USE_TEST_SPREADSHEET', 'False').lower() == 'true',
    'skip_slow_queries': os.getenv('DEBUG_SKIP_SLOW_QUERIES', 'False').lower() == 'true',
    'test_date': os.getenv('DEBUG_TEST_DATE', '2025-01-01')
}

# Google Sheets debug settings
SHEETS_DEBUG = {
    'dry_run': os.getenv('SHEETS_DRY_RUN', 'False').lower() == 'true',
    'verify_writes': os.getenv('SHEETS_VERIFY_WRITES', 'False').lower() == 'true',
    'log_ranges': os.getenv('SHEETS_LOG_RANGES', 'False').lower() == 'true'
}

# Logging configuration
LOG_FILE = os.getenv('LOG_FILE', 'kpi_automation.log')
LOG_MAX_BYTES = int(os.getenv('LOG_MAX_BYTES', str(5 * 1024 * 1024)))
LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', '3'))

# Data collection settings
DATA_COLLECTION = {
    'auto_detect_week': True,
    'manual_dates': {
        'start_date': '2025-01-01',
        'end_date': '2025-01-07'
    }
}


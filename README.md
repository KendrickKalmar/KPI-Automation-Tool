# KPI Automation Tool

Автоматизированный инструмент для сбора KPI метрик из PostgreSQL базы данных и размещения их в Google Sheets. Система поддерживает автоматический расчет недельных периодов, параллельное выполнение запросов и гибкую конфигурацию размещения данных.

## Возможности

- Автоматический сбор метрик из PostgreSQL
- Размещение данных в Google Sheets с автоопределением строк
- Параллельное выполнение SQL-запросов для ускорения работы
- Поддержка различных типов данных: таблицы, одиночные значения, формулы
- Автоматический расчет недельных периодов
- Гибкая конфигурация через переменные окружения
- Подробное логирование всех операций

## Требования

- Python 3.8+
- PostgreSQL база данных
- Google Cloud проект с включенным Google Sheets API
- Service Account для доступа к Google Sheets

## Установка

1. Клонируйте репозиторий:
```bash
git clone <repository-url>
cd SR_KPI_autocollection
```

2. Создайте виртуальное окружение:
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# или
venv\Scripts\activate  # Windows
```

3. Установите зависимости:
```bash
pip install -r requirements.txt
```

4. Настройте конфигурацию:
```bash
cp config.example.py config.py
```

Отредактируйте `config.py` или установите переменные окружения (см. раздел "Конфигурация").

## Настройка Google Sheets API

1. Создайте проект в [Google Cloud Console](https://console.cloud.google.com/)

2. Включите Google Sheets API:
   - Перейдите в "APIs & Services" > "Library"
   - Найдите "Google Sheets API" и включите его

3. Создайте Service Account:
   - Перейдите в "APIs & Services" > "Credentials"
   - Нажмите "Create Credentials" > "Service Account"
   - Создайте аккаунт и скачайте JSON ключ

4. Сохраните ключ:
   - Переименуйте скачанный файл в `google_credentials.json`
   - Поместите его в корневую директорию проекта

5. Предоставьте доступ к таблицам:
   - Откройте ваши Google Sheets
   - Нажмите "Share" (Поделиться)
   - Добавьте email из Service Account (находится в JSON файле, поле `client_email`)
   - Дайте права "Editor" (Редактор)

## Конфигурация

### Переменные окружения

Создайте файл `.env` или экспортируйте переменные:

```bash
# Database
export DB_HOST=your-db-host
export DB_PORT=5432
export DB_NAME=your-database
export DB_USER=your-username
export DB_PASSWORD=your-password

# Google Sheets
export SPREADSHEET_ID_SR_PLUS=your-spreadsheet-id-1
export SPREADSHEET_ID_DEPO_KPI=your-spreadsheet-id-2
export GOOGLE_CREDENTIALS_FILE=google_credentials.json

# Optional: Test mode
export DEBUG_MODE=False
export DEBUG_TEST_PERIOD_DAYS=7
export DEBUG_TEST_DATE=2025-01-01
```

### Структура Google Sheets

Проект ожидает следующие листы в ваших таблицах:

- **Services** - для метрик common_kpi и custody_deposits
- **TF_SR** - для метрик трансферов и выплат
- **status** - для статусов и формул
- **currency** - для статистики по валютам

Подробная конфигурация размещения данных находится в `config_placement.py`.

## Использование

### Базовый запуск

```bash
python auto_collect.py
```

Скрипт автоматически:
- Определит предыдущую неделю (понедельник-воскресенье)
- Выполнит все SQL-запросы
- Разместит данные в соответствующие листы Google Sheets

### Тестовый режим

Для тестирования на конкретном периоде:

```bash
export DEBUG_MODE=True
export DEBUG_TEST_DATE=2025-01-06
export DEBUG_TEST_PERIOD_DAYS=7
python auto_collect.py
```

### Автоматический запуск (cron)

Для еженедельного запуска добавьте в crontab:

```bash
# Каждый понедельник в 9:00
0 9 * * 1 cd /path/to/SR_KPI_autocollection && /path/to/venv/bin/python auto_collect.py >> /path/to/logs/cron.log 2>&1
```

Или используйте встроенный планировщик:

```bash
python scheduler.py
```

## Структура проекта

```
SR_KPI_autocollection/
├── auto_collect.py          # Основной скрипт сбора данных
├── config.py                # Конфигурация (не коммитить!)
├── config.example.py        # Пример конфигурации
├── config_placement.py      # Конфигурация размещения в Sheets
├── data_processing.py       # Обработка данных из БД
├── database.py              # Работа с PostgreSQL
├── sheet_placement.py       # Размещение данных в Google Sheets
├── parallel_executor.py     # Параллельное выполнение запросов
├── query_config.py          # SQL-запросы
├── debug_utils.py           # Утилиты для отладки
├── scheduler.py             # Планировщик задач
├── requirements.txt         # Зависимости Python
└── README.md               # Документация
```

## Логирование

Все операции логируются в файл `kpi_automation.log` с ротацией:
- Максимальный размер: 5 MB
- Количество резервных копий: 3

Для более подробного логирования установите:
```bash
export SHEETS_VERIFY_WRITES=True
export SHEETS_LOG_RANGES=True
```

## Настройка SQL-запросов

SQL-запросы настраиваются в `query_config.py`. Каждый запрос должен:
- Использовать плейсхолдеры `:start_week`, `:end_week`, `:end_week_exclusive`
- Возвращать данные в формате, ожидаемом обработчиками в `data_processing.py`

## Настройка размещения данных

Конфигурация размещения находится в `config_placement.py`. Для каждого типа данных указывается:
- ID таблицы Google Sheets
- Имя листа
- Тип данных (table, single_value, common_kpi_table, statuses_shift)
- Размещение (ячейки или диапазоны)

## Отладка

### Режим dry-run

Проверка без записи в Sheets:
```bash
export SHEETS_DRY_RUN=True
python auto_collect.py
```

### Пропуск медленных запросов

В тестовом режиме можно пропустить медленные запросы:
```bash
export DEBUG_SKIP_SLOW_QUERIES=True
```

## Устранение неполадок

### Ошибка подключения к БД
- Проверьте правильность учетных данных в `config.py`
- Убедитесь, что БД доступна с вашего IP

### Ошибка доступа к Google Sheets
- Проверьте, что Service Account имеет доступ к таблицам
- Убедитесь, что `google_credentials.json` находится в корне проекта
- Проверьте, что Google Sheets API включен в проекте

### Пустые данные в Sheets
- Проверьте логи на наличие ошибок SQL-запросов
- Убедитесь, что период содержит данные в БД
- Проверьте конфигурацию размещения в `config_placement.py`

## Лицензия

[Укажите лицензию]

## Автор

[Ваше имя/команда]


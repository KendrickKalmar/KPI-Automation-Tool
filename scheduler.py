import schedule
import time
from auto_collect import main
import logging

# Настройка планировщика
schedule.every().day.at("09:00").do(main)  # Ежедневно в 9:00

logging.info("Планировщик запущен. Ожидание выполнения...")

while True:
    schedule.run_pending()
    time.sleep(60)  # Проверяем каждую минуту
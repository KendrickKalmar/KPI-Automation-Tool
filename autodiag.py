import subprocess
import time
import os
import re

LOG_NAME = "kpi_automation.log"
PY_RUN = "python3 auto_collect.py"


def read_log(logfile):
    with open(logfile, encoding="utf-8") as f:
        return f.readlines()

def scan_head(lines, start, width=10):
    return ''.join(lines[start:start+width])

def diagnose_log(logfile):
    lines = read_log(logfile)
    result = []
    for i, l in enumerate(lines):
        if ('Выполняем запрос:' in l or 'Результаты' in l or 'verify' in l or 'WARNING' in l) \
            and not any(s in l for s in ["INFO - Обрабатываем день", "INFO - Выполнено запросов"]):
            result.append((i, l.strip()))
    # Поиск ошибок verify
    verify_errs = [l for l in lines if 'verify' in l and ("ошибка" in l.lower() or "error" in l.lower() or "None" in l)]
    df_warnings = [l for l in lines if 'WARNING' in l or 'пусто' in l or 'не найдена' in l or 'ошибка' in l]
    
    # Быстрое сведение по head каждой выгрузки:
    print("\n==== DIAGNOSTICS ====")
    for idx, l in result:
        print(f"  {l}")
        scan = scan_head(lines, idx+1, 8)
        if scan: print(scan)
    print("\n== VERIFY FAILS ==")
    for v in verify_errs: print(v.strip())
    if not verify_errs:
        print("  -- verify OK everywhere")
    print("\n== DF WARNINGS ==")
    for w in df_warnings: print(w.strip())
    if not df_warnings:
        print("  -- no dataframe warnings/errors detected")
    print("==== END ====")


def main():
    print(f"Запуск: {PY_RUN}")
    proc = subprocess.run(PY_RUN, shell=True)
    if proc.returncode != 0:
        print(f"Ошибка запуска auto_collect.py! Code={proc.returncode}")
        return
    if not os.path.exists(LOG_NAME):
        print(f"Лог {LOG_NAME} не найден!")
        return
    time.sleep(2) # на всякий случай, чтобы файлы логов сбросились
    diagnose_log(LOG_NAME)


if __name__ == "__main__":
    main()

@echo off
cd /d "%~dp0.."
if exist "quant\Scripts\python.exe" (
    quant\Scripts\python.exe scripts\daily_official_5m_compare.py >> logs\daily_compare.log 2>&1
) else if exist "quant\bin\python.exe" (
    quant\bin\python.exe scripts\daily_official_5m_compare.py >> logs\daily_compare.log 2>&1
) else (
    python scripts\daily_official_5m_compare.py >> logs\daily_compare.log 2>&1
)

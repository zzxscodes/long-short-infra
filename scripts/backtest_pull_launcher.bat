@echo off
REM Backtest pull+compare launcher (Windows)
REM Pull data from live machine, compare vs official, fix mismatches.
cd /d "%~dp0.."
if not exist "logs" mkdir logs

REM Write to a per-run log file to avoid file locking issues.
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%i"
set "LOG_FILE=logs\backtest_pull_compare_%TS%.log"

if exist "quant\Scripts\python.exe" (
    quant\Scripts\python.exe -u scripts\backtest_pull_compare.py --print-fail >> "%LOG_FILE%" 2>&1
) else if exist "quant\python.exe" (
    quant\python.exe -u scripts\backtest_pull_compare.py --print-fail >> "%LOG_FILE%" 2>&1
) else (
    python -u scripts\backtest_pull_compare.py --print-fail >> "%LOG_FILE%" 2>&1
)

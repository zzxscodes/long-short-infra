@echo off
REM 回测机数据拉取对比启动器（Windows）
REM 从实盘机器拉取数据，与官方数据对比，不一致则自动修复
cd /d "%~dp0.."
if not exist "logs" mkdir logs
if exist "quant\Scripts\python.exe" (
    quant\Scripts\python.exe scripts\backtest_pull_compare.py --print-fail >> logs\backtest_pull_compare.log 2>&1
) else if exist "quant\python.exe" (
    quant\python.exe scripts\backtest_pull_compare.py --print-fail >> logs\backtest_pull_compare.log 2>&1
) else (
    python scripts\backtest_pull_compare.py --print-fail >> logs\backtest_pull_compare.log 2>&1
)

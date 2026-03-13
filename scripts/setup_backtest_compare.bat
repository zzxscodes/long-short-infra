@echo off
REM 安装回测机数据拉取对比定时任务（Windows）
REM 从实盘机器拉取数据，与官方 Binance 数据对比，不一致则自动修复
REM 用法: scripts\setup_backtest_compare.bat [时间] [--remove]
REM       时间格式 24 小时 HH:MM，如 10:30 或 18:30。不传则用下方默认 RUN_AT
setlocal

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
set "LAUNCHER=%PROJECT_ROOT%\scripts\backtest_pull_launcher.bat"
set "LOG_DIR=%PROJECT_ROOT%\logs"
set "TASK_NAME=BacktestPullCompare"
REM 默认 18:30（UTC+8 对应 UTC 10:30）。可改为本地时间
set "RUN_AT=18:30"

if "%1"=="" goto install
if "%1"=="--remove" goto remove
if "%1"=="-r" goto remove
if "%1"=="--help" goto help
if "%1"=="-h" goto help
REM 若第一个参数形如 10:30 或 18:30，视为运行时间
echo %1 | findstr /R "^[0-9][0-9]*:[0-9][0-9]*$" >nul 2>&1
if %errorlevel% equ 0 set "RUN_AT=%1"
goto install

:help
echo Usage: scripts\setup_backtest_compare.bat [HH:MM] [--remove ^| --help]
echo   HH:MM    每日运行时间（24 小时），如 10:30 或 18:30。不传则用默认 18:30
echo   --remove  移除定时任务
echo   --help    显示帮助
echo.
echo 此脚本在回测机（Windows）上安装定时任务：
echo   1. 从实盘机器拉取 klines/funding_rates/premium_index 数据
echo   2. 与 Binance 官方数据（data.binance.vision）对比
echo   3. 对比不通过则自动下载 aggTrade 重新聚合修复
echo.
echo 前置条件：
echo   1. 配置 SSH 免密登录到实盘机器（ssh-keygen + ssh-copy-id）
echo   2. 在 config/default.yaml 中配置 backtest_pull 相关参数
exit /b 0

:install
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
schtasks /query /tn "%TASK_NAME%" >nul 2>&1
if %errorlevel% equ 0 (
    echo 定时任务已存在，先删除再创建
    schtasks /delete /tn "%TASK_NAME%" /f
)
schtasks /create /tn "%TASK_NAME%" /tr "%LAUNCHER%" /sc daily /st %RUN_AT% /f
if %errorlevel% equ 0 (
    echo 已安装定时任务: 每日 %RUN_AT% 执行（系统时区）
    echo 若需 UTC 10:30，请将时间设为本地对应时刻（如 UTC+8 用 18:30）
) else (
    echo 安装失败
    exit /b 1
)
goto end

:remove
schtasks /query /tn "%TASK_NAME%" >nul 2>&1
if %errorlevel% neq 0 (
    echo 未找到定时任务 %TASK_NAME%
    goto end
)
schtasks /delete /tn "%TASK_NAME%" /f
echo 已移除定时任务

:end
endlocal

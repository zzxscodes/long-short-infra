@echo off
REM 安装每日官方 K 线对比定时任务（Windows）
REM 使用 data.binance.vision。schtasks 使用系统时区，可设置运行时间（如 UTC+8 对应 10:30 UTC 则设为 18:30）
REM 用法: scripts\setup_daily_compare.bat [时间] [--remove]
REM       时间格式 24 小时 HH:MM，如 10:30 或 18:30。不传则用下方默认 RUN_AT
setlocal

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
set "LAUNCHER=%PROJECT_ROOT%\scripts\daily_compare_launcher.bat"
set "LOG_DIR=%PROJECT_ROOT%\logs"
set "TASK_NAME=LongShortDailyCompare"
REM 默认 10:30（若系统时区为 UTC 即 UTC 10:30）。可改为本地时间，如 UTC+8 用 18:30
set "RUN_AT=10:30"

if "%1"=="" goto install
if "%1"=="--remove" goto remove
if "%1"=="-r" goto remove
if "%1"=="--help" goto help
if "%1"=="-h" goto help
REM 若第一个参数形如 10:30 或 18:30，视为运行时间（支持 1:30 或 09:05）
echo %1 | findstr /R "^[0-9][0-9]*:[0-9][0-9]*$" >nul 2>&1
if %errorlevel% equ 0 set "RUN_AT=%1"
goto install

:help
echo Usage: scripts\setup_daily_compare.bat [HH:MM] [--remove ^| --help]
echo   HH:MM    每日运行时间（24 小时），如 10:30 或 18:30。不传则用默认 10:30
echo   --remove  移除定时任务
echo   --help    显示帮助
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
    echo 若需 UTC 10:30，请将系统时区设为 UTC 或将时间设为本地对应时刻
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

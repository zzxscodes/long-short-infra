#!/usr/bin/env python3
"""
跨平台安装回测机数据拉取对比定时任务

- Windows: 创建 schtasks，可设置时间（默认 18:30 本地时间，对应 UTC+8 的 UTC 10:30）
- Linux/macOS: 写入 crontab，可设置时间（默认 UTC 10:30，配合 CRON_TZ=UTC）

用法:
  python scripts/setup_backtest_compare_scheduler.py [--time HH:MM]     # 安装
  python scripts/setup_backtest_compare_scheduler.py --remove          # 移除
  python scripts/setup_backtest_compare_scheduler.py --time 18:30       # 每日 18:30
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "scripts" / "backtest_pull_compare.py"
LOG_FILE = PROJECT_ROOT / "logs" / "backtest_pull_compare.log"
TASK_NAME = "BacktestPullCompare"
CRON_COMMENT = "# long-short-infra: backtest pull compare (data.binance.vision)"
DEFAULT_TIME_WINDOWS = "18:30"
DEFAULT_TIME_UNIX = "10:30"


def _parse_time(hhmm: str) -> tuple[int, int]:
    """解析 HH:MM 或 H:MM，返回 (hour, minute)。非法则抛出 ValueError"""
    m = re.match(r"^([0-9]|[01]?[0-9]|2[0-3]):([0-5]?[0-9])$", hhmm.strip())
    if not m:
        raise ValueError(f"时间格式须为 HH:MM（24 小时），如 10:30、18:30，当前: {hhmm!r}")
    return int(m.group(1)), int(m.group(2))


def _install_unix(run_at: str) -> bool:
    hour, minute = _parse_time(run_at)
    python_bin = PROJECT_ROOT / "quant" / "bin" / "python3"
    if not python_bin.exists():
        python_bin = PROJECT_ROOT / "quant" / "bin" / "python"
    if not python_bin.exists():
        python_bin = Path("python3")
    run_cmd = f"cd {PROJECT_ROOT} && {python_bin} scripts/backtest_pull_compare.py --print-fail >> {LOG_FILE} 2>&1"
    cron_tz = "CRON_TZ=UTC"
    cron_entry = f"{minute} {hour} * * * {run_cmd}"
    try:
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        current = result.stdout if result.returncode == 0 else ""
        if "backtest_pull_compare" in current and CRON_COMMENT in current:
            print("定时任务已存在，跳过安装")
            return True
        lines = [
            line for line in current.splitlines()
            if "backtest_pull_compare" not in line and CRON_COMMENT not in line and line.strip() != "CRON_TZ=UTC"
        ]
        new_crontab = "\n".join(lines).rstrip() + f"\n\n{CRON_COMMENT}\n{cron_tz}\n{cron_entry}\n"
        proc = subprocess.run(["crontab", "-"], input=new_crontab, capture_output=True, text=True)
        if proc.returncode != 0:
            print(f"crontab 失败: {proc.stderr}")
            return False
        print(f"已安装定时任务: 每日 {run_at} UTC 执行（CRON_TZ=UTC）")
        print(f"  {cron_entry}")
        return True
    except FileNotFoundError:
        print("未找到 crontab 命令")
        return False


def _remove_unix() -> bool:
    try:
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        if result.returncode != 0:
            print("当前无 crontab")
            return True
        current = result.stdout
        if "backtest_pull_compare" not in current:
            print("未找到匹配的定时任务")
            return True
        lines = [
            line for line in current.splitlines()
            if "backtest_pull_compare" not in line and CRON_COMMENT not in line and line.strip() != "CRON_TZ=UTC"
        ]
        new_crontab = "\n".join(lines).rstrip() + "\n"
        subprocess.run(["crontab", "-"], input=new_crontab, capture_output=True, check=True)
        print("已移除定时任务")
        return True
    except Exception as e:
        print(f"移除失败: {e}")
        return False


def _install_windows(run_at: str) -> bool:
    hour, minute = _parse_time(run_at)
    run_at_st = f"{hour:02d}:{minute:02d}"
    launcher = (PROJECT_ROOT / "scripts" / "backtest_pull_launcher.bat").resolve()
    tr_cmd = f'"{launcher}"'
    try:
        subprocess.run(["schtasks", "/query", "/tn", TASK_NAME], capture_output=True, check=False)
        subprocess.run(["schtasks", "/delete", "/tn", TASK_NAME, "/f"], capture_output=True, check=False)
    except Exception:
        pass
    proc = subprocess.run(
        ["schtasks", "/create", "/tn", TASK_NAME, "/tr", tr_cmd, "/sc", "daily", "/st", run_at_st, "/f"],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        print(f"schtasks 失败: {proc.stderr}")
        return False
    print(f"已安装定时任务: 每日 {run_at_st} 执行（系统时区）")
    print("若需 UTC 10:30，请将系统时区设为 UTC 或将 --time 设为本地对应时刻（如 UTC+8 用 18:30）")
    return True


def _remove_windows() -> bool:
    proc = subprocess.run(["schtasks", "/query", "/tn", TASK_NAME], capture_output=True)
    if proc.returncode != 0:
        print("未找到定时任务")
        return True
    subprocess.run(["schtasks", "/delete", "/tn", TASK_NAME, "/f"], capture_output=True, check=True)
    print("已移除定时任务")
    return True


def main() -> int:
    is_windows = sys.platform.startswith("win")
    default_time = DEFAULT_TIME_WINDOWS if is_windows else DEFAULT_TIME_UNIX
    
    parser = argparse.ArgumentParser(
        description="跨平台安装/移除回测机数据拉取对比定时任务"
    )
    parser.add_argument("--remove", "-r", action="store_true", help="移除定时任务")
    parser.add_argument(
        "--time", "-t",
        default=default_time,
        metavar="HH:MM",
        help=f"每日运行时间（24 小时），如 10:30、18:30。默认 %(default)s（Unix 为 UTC，Windows 为系统时区）",
    )
    args = parser.parse_args()

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    if args.remove:
        ok = _remove_windows() if is_windows else _remove_unix()
    else:
        try:
            _parse_time(args.time)
        except ValueError as e:
            print(e)
            return 1
        ok = _install_windows(args.time) if is_windows else _install_unix(args.time)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

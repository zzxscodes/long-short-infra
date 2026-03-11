#!/bin/bash
#
# 安装每日官方 K 线对比定时任务（Linux/macOS）
# 使用 data.binance.vision，可设置每日运行时间（默认 UTC 10:30，CRON_TZ=UTC）
# 用法:
#   ./scripts/setup_daily_compare_cron.sh [HH:MM]     # 安装，默认 10:30 UTC
#   ./scripts/setup_daily_compare_cron.sh 18:30       # 每日 18:30 UTC
#   ./scripts/setup_daily_compare_cron.sh --remove     # 移除
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="${PROJECT_ROOT}/quant/bin/python3"
CRON_CMD="cd $PROJECT_ROOT && $PYTHON_BIN scripts/daily_official_5m_compare.py >> logs/daily_compare.log 2>&1"
CRON_TZ_LINE="CRON_TZ=UTC"

# 解析时间：默认 10:30 UTC；若第一个参数为 HH:MM 则使用
CRON_HOUR=10
CRON_MIN=30
if [[ -n "${1:-}" && "$1" =~ ^([0-9]|1[0-9]|2[0-3]):([0-5]?[0-9])$ ]]; then
    CRON_HOUR="${BASH_REMATCH[1]}"
    CRON_MIN="${BASH_REMATCH[2]}"
fi
CRON_ENTRY="${CRON_MIN} ${CRON_HOUR} * * * $CRON_CMD"
CRON_COMMENT="# long-short-infra: daily official 5m compare (data.binance.vision) UTC ${CRON_HOUR}:${CRON_MIN}"

remove_cron() {
    if crontab -l 2>/dev/null | grep -F "daily_official_5m_compare" >/dev/null 2>&1; then
        crontab -l 2>/dev/null | grep -v "daily_official_5m_compare" | grep -v -F "$CRON_COMMENT" | grep -v "^CRON_TZ=UTC$" | crontab - 2>/dev/null || true
        echo "已移除定时任务"
    else
        echo "未找到匹配的定时任务"
    fi
}

install_cron() {
    mkdir -p "$PROJECT_ROOT/logs"
    if crontab -l 2>/dev/null | grep -F "$CRON_COMMENT" >/dev/null 2>&1; then
        echo "定时任务已存在，跳过安装"
        crontab -l | grep -A2 "$CRON_COMMENT"
    else
        (crontab -l 2>/dev/null; echo ""; echo "$CRON_COMMENT"; echo "$CRON_TZ_LINE"; echo "$CRON_ENTRY") | crontab -
        echo "已安装定时任务: 每日 ${CRON_HOUR}:${CRON_MIN} UTC 执行"
        echo "  $CRON_ENTRY"
    fi
}

case "${1:-}" in
    --remove|-r)
        remove_cron
        ;;
    --help|-h)
        echo "Usage: $0 [HH:MM] [--remove|--help]"
        echo "  HH:MM    每日运行时间（UTC），24 小时制，如 10:30、18:30。不传则默认 10:30"
        echo "  --remove  移除已安装的定时任务"
        echo "  --help    显示帮助"
        exit 0
        ;;
    *)
        install_cron
        ;;
esac

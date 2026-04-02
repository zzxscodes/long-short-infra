#!/usr/bin/env bash
# Safe pkill: parent shell argv must not contain prepare_backtest_history_data.py
exec pkill -9 -f '[p]ython3.*scripts/prepare_backtest_history_data.py'

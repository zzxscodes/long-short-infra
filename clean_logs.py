"""
清理日志脚本
清理日志文件，以及短期数据（策略报告、权益曲线、持仓历史、持仓、信号等）
保留长期数据（如资金费率、K线、逐笔成交、溢价指数、Universe等）

- 长期数据（保留）：funding_rates, klines, trades, premium_index, universe
- 短期数据（清理）：equity_curve, position_history, positions, signals, strategy_reports
"""
import os
from pathlib import Path

def clear_directory(dir_path: Path):
    """清空目录内容，保留顶层目录结构"""
    if not dir_path.exists():
        return 0
    
    count = 0
    # 递归删除所有文件和子目录
    for item in dir_path.rglob('*'):
        try:
            if item.is_file():
                item.unlink()
                count += 1
            elif item.is_dir():
                # 先尝试删除目录（如果为空会自动删除）
                try:
                    item.rmdir()
                except OSError:
                    # 目录不为空，稍后处理
                    pass
        except Exception as e:
            print(f"Warning: Failed to delete {item}: {e}")
    
    # 删除所有空子目录（从最深开始）
    for root, dirs, files in os.walk(dir_path, topdown=False):
        root_path = Path(root)
        if root_path != dir_path:  # 不删除顶层目录
            try:
                if not any(root_path.iterdir()):  # 如果目录为空
                    root_path.rmdir()
            except Exception:
                pass
    
    return count

def clean_logs():
    """清理日志文件和短期数据"""
    script_dir = Path(__file__).parent
    
    # 清理日志目录
    logs_dir = script_dir / "logs"
    if logs_dir.exists():
        log_files = list(logs_dir.glob("*.log")) + list(logs_dir.glob("*_*.log"))
        cleaned_count = 0
        for log_file in log_files:
            try:
                log_file.unlink()
                cleaned_count += 1
            except Exception as e:
                print(f"Warning: Failed to delete {log_file}: {e}")
        print(f"Cleaned {cleaned_count} log files")
    else:
        print("Logs directory not found")
    
    # 短期数据目录列表（这些数据会在系统运行时重新生成）
    short_term_dirs = [
        "data/equity_curve",
        "data/position_history",
        "data/positions",
        "data/signals",
        "data/strategy_reports",
        "data/api_performance",
        "data/performance",
    ]
    
    print("\nClearing short-term data directories...")
    total_cleaned = 0
    for data_dir in short_term_dirs:
        dir_path = script_dir / data_dir
        if dir_path.exists():
            count = clear_directory(dir_path)
            total_cleaned += count
            print(f"  Cleared {data_dir} ({count} files)")
        else:
            print(f"  {data_dir} does not exist, skipping...")
    
    # 数据保留说明
    print("\nData directories:")
    print("  ✅ Long-term data (preserved): funding_rates, klines, trades, premium_index, universe")
    print("  🔄 Short-term data (cleaned): equity_curve, position_history, positions, signals, strategy_reports")
    print(f"\nTotal: {total_cleaned} files cleaned from short-term data directories")

if __name__ == "__main__":
    clean_logs()

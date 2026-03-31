"""
实盘与回测共用的 Alpha 快照落盘：统一 JSONL + 批量刷盘，降低高并发回测时的 IO 压力。

与 AlphaEngine / MultiFactorBacktest 输出格式对齐，便于对账与复现。
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from ..common.config import config
from ..common.logger import get_logger

logger = get_logger("alpha_snapshot_io")


def ts_iso(ts: datetime) -> str:
    t = pd.to_datetime(ts)
    if t.tzinfo is None:
        t = t.tz_localize(timezone.utc)
    else:
        t = t.tz_convert(timezone.utc)
    return t.isoformat()


@dataclass
class AlphaSnapshotRunWriter:
    """
    单次回测 run 的缓冲写入：每 flush_every 条或显式 flush 时追加写入 JSONL，并维护 alpha_index.json。
    读侧（增量续跑）可一次性加载 alpha_records.jsonl 构建时间戳 -> 权重。
    """

    run_name: str
    base_dir: Path
    universe_version: str
    flush_every: int = 50

    def __post_init__(self) -> None:
        self.run_dir = (self.base_dir / self.universe_version / self.run_name).resolve()
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.jsonl_path = self.run_dir / "alpha_records.jsonl"
        self.index_path = self.run_dir / "alpha_index.json"
        self._buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._index: Set[str] = set()
        self._load_index()
        # JSONL 存在但无 index 时，从记录重建索引（避免增量续跑误判为缺失）
        if self.jsonl_path.exists() and not self._index:
            for k in self.load_weights_by_ts().keys():
                self._index.add(k)

    def _load_index(self) -> None:
        if not self.index_path.exists():
            return
        try:
            data = json.loads(self.index_path.read_text(encoding="utf-8"))
            self._index = set(data.get("timestamps", []))
        except Exception as e:
            logger.warning(f"Failed to load alpha index {self.index_path}: {e}")

    def load_weights_by_ts(self) -> Dict[str, Dict[str, float]]:
        """从已有 JSONL 构建 open_time_iso -> weights（增量续跑时调用一次）。"""
        out: Dict[str, Dict[str, float]] = {}
        if not self.jsonl_path.exists():
            return out
        try:
            with open(self.jsonl_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    ot = rec.get("open_time")
                    w = rec.get("weights")
                    if isinstance(ot, str) and isinstance(w, dict):
                        out[ot] = {str(k): float(v) for k, v in w.items()}
        except Exception as e:
            logger.warning(f"Failed to scan {self.jsonl_path}: {e}")
        return out

    def has_timestamp(self, ts: datetime) -> bool:
        return ts_iso(ts) in self._index

    def record(
        self,
        ts: datetime,
        weights: Dict[str, float],
        *,
        per_calculator: Optional[Dict[str, Dict[str, float]]] = None,
        source: str = "backtest",
    ) -> None:
        rec = {
            "open_time": ts_iso(ts),
            "source": source,
            "weights": {str(k): float(v) for k, v in weights.items()},
        }
        if per_calculator:
            rec["per_calculator"] = {
                name: {str(k): float(v) for k, v in vec.items()}
                for name, vec in per_calculator.items()
            }
        with self._lock:
            self._buffer.append(rec)
            if len(self._buffer) >= max(1, int(self.flush_every)):
                self._flush_unlocked()

    def flush(self) -> None:
        with self._lock:
            self._flush_unlocked()

    def _flush_unlocked(self) -> None:
        if not self._buffer:
            return
        with open(self.jsonl_path, "a", encoding="utf-8") as f:
            for rec in self._buffer:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        for rec in self._buffer:
            self._index.add(rec["open_time"])
        self._buffer.clear()
        try:
            self.index_path.write_text(
                json.dumps({"timestamps": sorted(self._index)}, ensure_ascii=False, indent=0),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning(f"Failed to write alpha index {self.index_path}: {e}")


_live_lock = threading.Lock()


def maybe_persist_live_alpha_snapshot(
    *,
    weights: Dict[str, float],
    per_calculator: Dict[str, Dict[str, float]],
    begin_label: Optional[str],
    end_label: Optional[str],
    mode: str,
) -> None:
    """
    实盘 Alpha 每轮计算完成后追加一行 JSONL（默认每次 flush，IO 量小）。
    关闭：strategy.alpha.persist_enabled: false
    """
    if not config.get("strategy.alpha.persist_enabled", False):
        return
    root = Path(config.get("strategy.alpha.persist_directory", "data/alpha_snapshots/live"))
    root.mkdir(parents=True, exist_ok=True)
    path = root / "live_alpha.jsonl"
    rec = {
        "kind": "live_alpha",
        "written_at": datetime.now(timezone.utc).isoformat(),
        "begin_label": begin_label,
        "end_label": end_label,
        "mode": mode,
        "weights": {str(k): float(v) for k, v in weights.items()},
        "per_calculator": {
            name: {str(k): float(v) for k, v in vec.items()}
            for name, vec in per_calculator.items()
        },
    }
    line = json.dumps(rec, ensure_ascii=False) + "\n"
    try:
        with _live_lock:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
    except Exception as e:
        logger.warning(f"Live alpha persist failed: {e}", exc_info=True)

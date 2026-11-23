"""位点存储路由：在旧 JSON 布局与带版本的数据库之间分派位点读写。

迁移期间两种布局并存——同一轮迁移里，已确认的来源（A）读写走新数据库，
未迁移的来源（B）继续读写旧 JSON 文件与 JSONL 历史。本模块负责：

- 读写路由：按最新迁移记录的状态把读写分派到对应存储；
- 迁移：把每源 JSON 位点与历史 JSONL 采集记录搬进带版本的数据库，
  单源一个事务，中断后可断点续传，重复执行幂等；
- 回退：把新库里已确认的位点写回旧读取路径，新布局期间产生的历史
  回填旧 JSONL，迁移记录标记为已回退但保留可审计；
- 核对：迁移前后都能校验各源已交付范围、历史行不丢不重。
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from app.services.base import DATA_ROOT
from app.services.checkpoint_store import CheckpointStoreDB, _as_int, history_natural_key

STORAGE_JSON = "json_file"
STORAGE_DB = "versioned_db"


class MigrationInterrupted(RuntimeError):
    """迁移中断：携带已完成的来源和待续传的来源，供断点恢复。"""

    def __init__(
        self,
        message: str,
        *,
        round_id: str,
        completed: list[dict[str, Any]],
        remaining: list[str],
    ) -> None:
        super().__init__(message)
        self.round_id = round_id
        self.completed = completed
        self.remaining = remaining


class CheckpointRouter:
    """位点读写路由器：旧 JSON 布局 <-> 带版本的数据库。"""

    def __init__(self, data_root: Path | str) -> None:
        self.data_root = Path(data_root)
        self.db = CheckpointStoreDB(self.checkpoints_dir / "checkpoint_store.db")

    # -- 路径与旧布局文件 IO（与 base 的 JSON 位点格式保持一致） --

    @property
    def checkpoints_dir(self) -> Path:
        return self.data_root / "checkpoints"

    @property
    def history_file(self) -> Path:
        return self.checkpoints_dir / "checkpoint_history.jsonl"

    def read_json_checkpoint(self, name: str) -> dict[str, Any] | None:
        cp_file = self.checkpoints_dir / f"{name}.json"
        if not cp_file.exists():
            return None
        with open(cp_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def write_json_checkpoint(self, name: str, state: dict[str, Any]) -> Path:
        self.checkpoints_dir.mkdir(parents=True, exist_ok=True)
        cp_file = self.checkpoints_dir / f"{name}.json"
        with open(cp_file, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2, default=str)
        return cp_file

    def read_json_history(self) -> list[dict[str, Any]]:
        if not self.history_file.exists():
            return []
        records = []
        with open(self.history_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return records

    def append_json_history(self, entries: list[dict[str, Any]]) -> int:
        self.checkpoints_dir.mkdir(parents=True, exist_ok=True)
        written = 0
        with open(self.history_file, "a", encoding="utf-8") as f:
            for entry in entries:
                f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
                written += 1
        return written

    # -- 读写路由 --

    def storage_for(self, name: str) -> str:
        """该位点当前生效的存储：已确认迁移的走新库，其余走旧 JSON。"""
        record = self.db.latest_record(name)
        if record and record["status"] == "confirmed":
            return STORAGE_DB
        return STORAGE_JSON

    def read(self, name: str) -> dict[str, Any] | None:
        """按路由读取位点；新库缺失时回退旧布局，保证读不到时不丢位置。"""
        if self.storage_for(name) == STORAGE_DB:
            state = self.db.get_checkpoint(name)
            if state is not None:
                return state
        return self.read_json_checkpoint(name)

    def write(self, name: str, state: dict[str, Any]) -> Path:
        """按路由写入位点：已确认的来源写新布局，其余写旧 JSON。"""
        if self.storage_for(name) == STORAGE_DB:
            self.db.upsert_checkpoint(name, state)
            return self.db.db_path
        return self.write_json_checkpoint(name, state)

    def append_history(self, source_key: str, entry: dict[str, Any]) -> None:
        """追加采集历史：已确认的来源进新库，其余继续追加旧 JSONL。"""
        if self.storage_for(source_key) == STORAGE_DB:
            self.db.append_history([entry])
        else:
            self.append_json_history([entry])

    # -- 迁移 --

    def migrate_sources(
        self,
        source_keys: list[str],
        *,
        round_id: str | None = None,
        fail_after: int | None = None,
    ) -> dict[str, Any]:
        """把每源 JSON 位点与历史 JSONL 记录迁移到带版本的数据库。

        单源一个事务；已确认的来源自动跳过，因此中断后重跑即可续传，
        且不会重复记进度。``fail_after`` 用于演练中断：迁移完指定数量
        的来源后抛出 :class:`MigrationInterrupted`。
        """
        round_id = round_id or f"round-{int(time.time() * 1000)}"
        results: list[dict[str, Any]] = []
        migrated = 0

        for key in source_keys:
            latest = self.db.latest_record(key)
            if latest and latest["status"] == "confirmed":
                results.append({
                    "source_key": key,
                    "status": "already_confirmed",
                    "round_id": latest["round_id"],
                })
                continue

            # 存量原值：迁移这一刻旧 JSON 位点的完整快照
            original = self.read_json_checkpoint(key)
            history_rows = [r for r in self.read_json_history() if r.get("source_key") == key]

            if original is not None:
                state = dict(original)
            elif history_rows:
                last = history_rows[-1]
                state = {
                    "source_key": key,
                    "offset": _as_int(last.get("offset"), 0),
                    "line_number": _as_int(last.get("line_number"), 0),
                    "status": "active",
                }
            else:
                state = {"source_key": key, "offset": 0, "line_number": 0, "status": "active"}
            state["source_key"] = key
            state.setdefault("offset", 0)
            state.setdefault("line_number", 0)
            state.setdefault("status", "active")

            inserted = self.db.migrate_source_txn(
                round_id=round_id,
                source_key=key,
                state=state,
                original=original or {},
                history_rows=history_rows,
            )
            migrated += 1
            results.append({
                "source_key": key,
                "status": "confirmed",
                "round_id": round_id,
                "history_rows": len(history_rows),
                "history_inserted": inserted,
                "confirmed": {
                    "offset": _as_int(state.get("offset"), 0),
                    "line_number": _as_int(state.get("line_number"), 0),
                },
            })

            if fail_after is not None and migrated >= fail_after:
                remaining = [k for k in source_keys if k not in {
                    r["source_key"] for r in results
                }]
                raise MigrationInterrupted(
                    f"迁移轮次 {round_id} 在 {migrated} 个来源后中断，剩余 {len(remaining)} 个待续传",
                    round_id=round_id,
                    completed=results,
                    remaining=remaining,
                )

        return {"round_id": round_id, "results": results}

    def rollback(
        self,
        *,
        source_keys: list[str] | None = None,
        round_id: str | None = None,
    ) -> dict[str, Any]:
        """回退迁移：旧读取路径接续新库已确认的采集进度。

        对每个已确认的来源：取新库当前位点（含迁移期间推进的进度）写回
        旧 JSON；把新布局期间产生的历史回填旧 JSONL（按自然键去重）；
        迁移记录标记为已回退并保留，随时可审计、可再次迁移。
        """
        records = self.db.list_records(status="confirmed", round_id=round_id, source_keys=source_keys)
        results: list[dict[str, Any]] = []

        jsonl_rows = self.read_json_history()
        existing_keys = {history_natural_key(r) for r in jsonl_rows}
        pending_jsonl: list[dict[str, Any]] = []

        for record in reversed(records):
            key = record["source_key"]
            current = self.db.get_checkpoint(key) or {
                "source_key": key,
                "offset": record["confirmed"]["offset"],
                "line_number": record["confirmed"]["line_number"],
            }
            state = dict(current)
            state["source_key"] = key
            state["status"] = "rolled_back"
            state["rolled_back_at"] = time.time()
            self.write_json_checkpoint(key, state)

            # 新布局期间确认的历史回填旧 JSONL，保证旧路径不丢行、不重复
            for row in self.db.history_for(key):
                natural = history_natural_key(row)
                if natural in existing_keys:
                    continue
                existing_keys.add(natural)
                pending_jsonl.append(row)
            pending_jsonl.append({
                "source_key": key,
                "offset": _as_int(state.get("offset"), 0),
                "line_number": _as_int(state.get("line_number"), 0),
                "at": time.time(),
                "action": "rollback",
                "round_id": record["round_id"],
            })

            self.db.mark_rolled_back(record["id"], state)
            results.append({
                "source_key": key,
                "status": "rolled_back",
                "round_id": record["round_id"],
                "position": {
                    "offset": _as_int(state.get("offset"), 0),
                    "line_number": _as_int(state.get("line_number"), 0),
                },
            })

        if pending_jsonl:
            self.append_json_history(pending_jsonl)

        return {"rolled_back": len(results), "results": results}

    # -- 迁移记录与核对 --

    def records(self, source_key: str | None = None) -> list[dict[str, Any]]:
        """可回退的位点存储迁移记录：存量原值与新库确认范围的关联。"""
        if source_key is not None:
            record = self.db.latest_record(source_key)
            return [record] if record else []
        return self.db.list_records()

    def latest_record(self, source_key: str) -> dict[str, Any] | None:
        return self.db.latest_record(source_key)

    def verify(self, source_keys: list[str]) -> dict[str, Any]:
        """核对各源位点与历史：已交付范围一致、历史不丢行、不重复记进度。"""
        jsonl_rows = self.read_json_history()
        report: dict[str, Any] = {"consistent": True, "sources": {}}

        for key in source_keys:
            record = self.db.latest_record(key)
            current = self.read(key)
            old_rows = [r for r in jsonl_rows if r.get("source_key") == key]
            db_rows = self.db.history_for(key)
            old_keys = {history_natural_key(r) for r in old_rows}
            db_keys = {history_natural_key(r) for r in db_rows}

            problems: list[str] = []
            if record and record["status"] == "confirmed":
                # 旧 JSONL 里的每一行都必须已迁入新库
                missing = old_keys - db_keys
                if missing:
                    problems.append(f"旧历史有 {len(missing)} 行未迁入新库")
                if len(db_keys) != len(db_rows):
                    problems.append("新库历史存在重复进度")
                db_state = self.db.get_checkpoint(key)
                if db_state is None:
                    problems.append("新库缺少已确认的位点")
            elif record and record["status"] == "rolled_back":
                # 回退后新库确认的每一行都必须能在旧 JSONL 找到
                missing = db_keys - old_keys
                if missing:
                    problems.append(f"回退后旧历史缺少 {len(missing)} 行新库记录")
                if len(old_keys) != len(old_rows):
                    problems.append("旧历史存在重复进度")
                rollback_pos = record.get("rollback_position") or {}
                json_state = self.read_json_checkpoint(key) or {}
                if _as_int(json_state.get("offset"), -1) != _as_int(rollback_pos.get("offset"), -2):
                    problems.append("旧读取路径未接续回退确认的位点")

            entry = {
                "storage": self.storage_for(key),
                "position": {
                    "offset": _as_int(current.get("offset"), 0),
                    "line_number": _as_int(current.get("line_number"), 0),
                    "status": current.get("status"),
                } if current else None,
                "migration": record,
                "history": {
                    "json_file": len(old_rows),
                    "versioned_db": len(db_rows),
                },
                "problems": problems,
            }
            if problems:
                report["consistent"] = False
            report["sources"][key] = entry

        return report


_default_router: CheckpointRouter | None = None


def default_checkpoint_router() -> CheckpointRouter:
    """进程级默认路由器：绑定平台数据目录，供各服务共享。"""
    global _default_router
    if _default_router is None:
        _default_router = CheckpointRouter(DATA_ROOT)
    return _default_router

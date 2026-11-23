"""带版本的位点数据库（SQLite）。

把原先散落在 ``data/checkpoints/<源>.json`` 的每源 JSON 位点和
``checkpoint_history.jsonl`` 的历史采集记录，迁移进一个带 schema 版本的
SQLite 数据库：

- ``schema_migrations`` 记录已应用的结构版本，启动时逐版补齐；
- ``checkpoints`` 保存每个来源的最新位点（新布局）；
- ``checkpoint_history`` 保存迁移来的历史采集记录和上线后的新记录，
  以自然键唯一约束保证重复迁移不会产生重复进度；
- ``migration_records`` 保存可回退的位点存储迁移记录：存量原值、
  新库确认范围、回退落点，迁移与回退全程可核对。
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable

# 历史记录里允许缺省的数值字段统一用 -1 归一化，
# 避免 UNIQUE 约束里 NULL 互不相等导致重复行。
_MISSING = -1


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def history_natural_key(entry: dict[str, Any]) -> tuple:
    """历史采集记录的自然键：同一行无论落在 JSONL 还是数据库都应唯一。"""
    return (
        str(entry.get("source_key", "")),
        str(entry.get("action", "")),
        _as_float(entry.get("at"), 0.0),
        _as_int(entry.get("offset"), _MISSING),
        _as_int(entry.get("line_number"), _MISSING),
    )


class CheckpointStoreDB:
    """带版本的位点数据库。所有连接按操作打开，避免跨线程共享。"""

    SCHEMA: list[tuple[int, str, list[str]]] = [
        (
            1,
            "checkpoints 表：每源最新位点（新布局）",
            [
                """
                CREATE TABLE IF NOT EXISTS checkpoints (
                    source_key   TEXT PRIMARY KEY,
                    file_offset  INTEGER NOT NULL DEFAULT 0,
                    line_number  INTEGER NOT NULL DEFAULT 0,
                    status       TEXT NOT NULL DEFAULT 'active',
                    payload      TEXT NOT NULL DEFAULT '{}',
                    updated_at   REAL NOT NULL
                )
                """,
            ],
        ),
        (
            2,
            "checkpoint_history 与 migration_records 表",
            [
                """
                CREATE TABLE IF NOT EXISTS checkpoint_history (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_key   TEXT NOT NULL,
                    action       TEXT NOT NULL DEFAULT '',
                    file_offset  INTEGER NOT NULL DEFAULT -1,
                    line_number  INTEGER NOT NULL DEFAULT -1,
                    at           REAL NOT NULL DEFAULT 0,
                    payload      TEXT NOT NULL DEFAULT '{}',
                    UNIQUE (source_key, action, at, file_offset, line_number)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS migration_records (
                    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                    round_id               TEXT NOT NULL,
                    source_key             TEXT NOT NULL,
                    status                 TEXT NOT NULL,
                    original_payload       TEXT NOT NULL DEFAULT '{}',
                    confirmed_file_offset  INTEGER NOT NULL DEFAULT 0,
                    confirmed_line_number  INTEGER NOT NULL DEFAULT 0,
                    history_rows           INTEGER NOT NULL DEFAULT 0,
                    migrated_at            REAL NOT NULL,
                    rolled_back_at         REAL,
                    rollback_payload       TEXT,
                    UNIQUE (round_id, source_key)
                )
                """,
            ],
        ),
    ]

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self._ensure_schema()

    # -- schema 版本 --

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version    INTEGER PRIMARY KEY,
                    note       TEXT NOT NULL,
                    applied_at REAL NOT NULL
                )
                """
            )
            applied = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}
            for version, note, ddls in self.SCHEMA:
                if version in applied:
                    continue
                for ddl in ddls:
                    conn.execute(ddl)
                conn.execute(
                    "INSERT INTO schema_migrations (version, note, applied_at) VALUES (?, ?, ?)",
                    (version, note, time.time()),
                )
            conn.commit()

    def applied_versions(self) -> list[int]:
        with self._connect() as conn:
            rows = conn.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
        return [int(row[0]) for row in rows]

    def schema_version(self) -> int:
        versions = self.applied_versions()
        return versions[-1] if versions else 0

    # -- 位点（新布局） --

    @staticmethod
    def _upsert_checkpoint(conn: sqlite3.Connection, source_key: str, state: dict[str, Any]) -> None:
        payload = json.dumps(state, ensure_ascii=False, default=str)
        conn.execute(
            """
            INSERT INTO checkpoints (source_key, file_offset, line_number, status, payload, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_key) DO UPDATE SET
                file_offset = excluded.file_offset,
                line_number = excluded.line_number,
                status      = excluded.status,
                payload     = excluded.payload,
                updated_at  = excluded.updated_at
            """,
            (
                source_key,
                _as_int(state.get("offset"), 0),
                _as_int(state.get("line_number"), 0),
                str(state.get("status", "active")),
                payload,
                time.time(),
            ),
        )

    def upsert_checkpoint(self, source_key: str, state: dict[str, Any]) -> None:
        with self._connect() as conn:
            self._upsert_checkpoint(conn, source_key, state)
            conn.commit()

    def get_checkpoint(self, source_key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload FROM checkpoints WHERE source_key = ?", (source_key,)
            ).fetchone()
        if row is None:
            return None
        return json.loads(row["payload"])

    def list_checkpoints(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT payload FROM checkpoints ORDER BY source_key").fetchall()
        return [json.loads(row["payload"]) for row in rows]

    # -- 历史采集记录 --

    @staticmethod
    def _append_history(conn: sqlite3.Connection, entries: Iterable[dict[str, Any]]) -> int:
        inserted = 0
        for entry in entries:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO checkpoint_history
                    (source_key, action, file_offset, line_number, at, payload)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(entry.get("source_key", "")),
                    str(entry.get("action", "")),
                    _as_int(entry.get("offset"), _MISSING),
                    _as_int(entry.get("line_number"), _MISSING),
                    _as_float(entry.get("at"), 0.0),
                    json.dumps(entry, ensure_ascii=False, default=str),
                ),
            )
            inserted += cur.rowcount
        return inserted

    def append_history(self, entries: Iterable[dict[str, Any]]) -> int:
        """追加历史记录，按自然键去重，返回实际写入行数。"""
        with self._connect() as conn:
            inserted = self._append_history(conn, entries)
            conn.commit()
        return inserted

    def history_for(self, source_key: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT payload FROM checkpoint_history WHERE source_key = ? ORDER BY at, id",
                (source_key,),
            ).fetchall()
        return [json.loads(row["payload"]) for row in rows]

    def history_count(self, source_key: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM checkpoint_history WHERE source_key = ?", (source_key,)
            ).fetchone()
        return int(row[0])

    # -- 迁移记录（可回退） --

    @staticmethod
    def _record_from_row(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "round_id": row["round_id"],
            "source_key": row["source_key"],
            "status": row["status"],
            # 存量原值（迁移时旧 JSON 位点的完整快照）
            "original": json.loads(row["original_payload"] or "{}"),
            # 新数据库里确认的范围
            "confirmed": {
                "offset": row["confirmed_file_offset"],
                "line_number": row["confirmed_line_number"],
            },
            "history_rows": row["history_rows"],
            "migrated_at": row["migrated_at"],
            "rolled_back_at": row["rolled_back_at"],
            # 回退时写回旧读取路径的落点
            "rollback_position": json.loads(row["rollback_payload"])
            if row["rollback_payload"]
            else None,
        }

    @staticmethod
    def _insert_record(
        conn: sqlite3.Connection,
        *,
        round_id: str,
        source_key: str,
        original: dict[str, Any],
        confirmed: dict[str, Any],
        history_rows: int,
        status: str = "confirmed",
    ) -> None:
        conn.execute(
            """
            INSERT INTO migration_records
                (round_id, source_key, status, original_payload,
                 confirmed_file_offset, confirmed_line_number, history_rows, migrated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(round_id, source_key) DO UPDATE SET
                status                 = excluded.status,
                original_payload       = excluded.original_payload,
                confirmed_file_offset  = excluded.confirmed_file_offset,
                confirmed_line_number  = excluded.confirmed_line_number,
                history_rows           = excluded.history_rows,
                migrated_at            = excluded.migrated_at,
                rolled_back_at         = NULL,
                rollback_payload       = NULL
            """,
            (
                round_id,
                source_key,
                status,
                json.dumps(original, ensure_ascii=False, default=str),
                _as_int(confirmed.get("offset"), 0),
                _as_int(confirmed.get("line_number"), 0),
                history_rows,
                time.time(),
            ),
        )

    def latest_record(self, source_key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM migration_records WHERE source_key = ? ORDER BY id DESC LIMIT 1",
                (source_key,),
            ).fetchone()
        return self._record_from_row(row) if row else None

    def list_records(
        self,
        *,
        status: str | None = None,
        round_id: str | None = None,
        source_keys: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM migration_records"
        clauses: list[str] = []
        args: list[Any] = []
        if status is not None:
            clauses.append("status = ?")
            args.append(status)
        if round_id is not None:
            clauses.append("round_id = ?")
            args.append(round_id)
        if source_keys:
            clauses.append("source_key IN (%s)" % ",".join("?" for _ in source_keys))
            args.extend(source_keys)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id"
        with self._connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [self._record_from_row(row) for row in rows]

    def mark_rolled_back(self, record_id: int, rollback_state: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE migration_records
                SET status = 'rolled_back', rolled_back_at = ?, rollback_payload = ?
                WHERE id = ?
                """,
                (
                    time.time(),
                    json.dumps(rollback_state, ensure_ascii=False, default=str),
                    record_id,
                ),
            )
            conn.commit()

    # -- 事务：单个来源的迁移要么全部落库，要么全部不落 --

    def migrate_source_txn(
        self,
        *,
        round_id: str,
        source_key: str,
        state: dict[str, Any],
        original: dict[str, Any],
        history_rows: list[dict[str, Any]],
    ) -> int:
        """单源迁移事务：写新布局位点 + 迁移历史 + 登记迁移记录。"""
        with self._connect() as conn:
            self._upsert_checkpoint(conn, source_key, state)
            inserted = self._append_history(conn, history_rows)
            self._insert_record(
                conn,
                round_id=round_id,
                source_key=source_key,
                original=original,
                confirmed=state,
                history_rows=len(history_rows),
            )
            conn.commit()
        return inserted

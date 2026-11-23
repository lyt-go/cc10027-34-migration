"""位点存储迁移测试：旧 JSON 布局 -> 带版本的数据库（可回退）。

覆盖任务场景：同一轮迁移两个有历史记录的来源，处理中断后再次读取
并写入，然后回退，验证各自确认位置和成品清单没有丢行或重复记进度。
"""
import sys
from pathlib import Path

import pytest

# 确保 backend 在 sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.services.checkpoint_router import CheckpointRouter, MigrationInterrupted
from app.services.collect_checkpoint import CollectCheckpointService
from app.services.format_output import FormatOutputService
from app.services.log_source import LogSourceService
from app.store import store

SRC_A = "LOG_-9001"
SRC_B = "LOG_-9002"


def make_services(tmp_path):
    """在隔离目录上构建共享同一路由的位点/格式输出服务。"""
    router = CheckpointRouter(tmp_path)
    return (
        router,
        CollectCheckpointService(checkpoint_router=router),
        FormatOutputService(checkpoint_router=router),
    )


def seed_old_layout(router: CheckpointRouter) -> None:
    """两个有历史记录的来源：每源 JSON 位点 + JSONL 采集历史。"""
    router.write_json_checkpoint(SRC_A, {
        "source_key": SRC_A, "offset": 1000, "line_number": 10, "status": "active",
    })
    router.write_json_checkpoint(SRC_B, {
        "source_key": SRC_B, "offset": 2000, "line_number": 20, "status": "active",
    })
    router.append_json_history([
        {"source_key": SRC_A, "offset": 400, "line_number": 4, "at": 100.0, "action": "save"},
        {"source_key": SRC_A, "offset": 1000, "line_number": 10, "at": 200.0, "action": "commit"},
        {"source_key": SRC_B, "offset": 800, "line_number": 8, "at": 150.0, "action": "save"},
        {"source_key": SRC_B, "offset": 1600, "line_number": 16, "at": 250.0, "action": "save"},
        {"source_key": SRC_B, "offset": 2000, "line_number": 20, "at": 350.0, "action": "commit"},
    ])


def manifest_positions(manifest):
    return {e["source_key"]: e for e in manifest["sources"]}


# -- 带版本的数据库 --

def test_versioned_store_tracks_schema_versions(tmp_path):
    """数据库带版本：schema 逐版应用并留痕。"""
    router = CheckpointRouter(tmp_path)
    assert router.db.applied_versions() == [1, 2]
    assert router.db.schema_version() == 2
    # 重复打开不重复应用
    again = CheckpointRouter(tmp_path)
    assert again.db.applied_versions() == [1, 2]


# -- 迁移两个有历史记录的来源 --

def test_migrate_two_sources_with_history(tmp_path):
    """迁移两个有历史记录的来源：位点入新库，历史随行，原值关联确认范围。"""
    router, ckpt, fmt = make_services(tmp_path)
    seed_old_layout(router)

    # 迁移前清单：两个源都在旧布局
    before = fmt.get_delivery_manifest([SRC_A, SRC_B])
    entries = manifest_positions(before)
    assert entries[SRC_A]["position_storage"] == "json_file"
    assert entries[SRC_B]["position_storage"] == "json_file"
    assert entries[SRC_A]["delivered_range"] == {"start_line": 0, "end_line": 10, "offset": 1000}
    assert entries[SRC_B]["delivered_range"] == {"start_line": 0, "end_line": 20, "offset": 2000}

    result = ckpt.migrate_to_versioned_store([SRC_A, SRC_B], round_id="round-1")
    assert result["round_id"] == "round-1"
    assert {r["source_key"] for r in result["results"]} == {SRC_A, SRC_B}

    # 新库位点与历史
    assert router.db.get_checkpoint(SRC_A)["offset"] == 1000
    assert router.db.get_checkpoint(SRC_B)["line_number"] == 20
    assert router.db.history_count(SRC_A) == 2
    assert router.db.history_count(SRC_B) == 3

    # 存量原值与新库确认范围建立关联
    rec_a = ckpt.migration_records(SRC_A)[0]
    assert rec_a["status"] == "confirmed"
    assert rec_a["original"]["offset"] == 1000
    assert rec_a["confirmed"] == {"offset": 1000, "line_number": 10}
    rec_b = ckpt.migration_records(SRC_B)[0]
    assert rec_b["original"]["line_number"] == 20
    assert rec_b["confirmed"] == {"offset": 2000, "line_number": 20}

    # 迁移后清单：两个源都切到新布局，已交付范围不变
    after = fmt.get_delivery_manifest([SRC_A, SRC_B])
    assert after["on_versioned_db"] == 2
    entries = manifest_positions(after)
    assert entries[SRC_A]["delivered_range"]["offset"] == 1000
    assert entries[SRC_B]["delivered_range"]["end_line"] == 20
    assert entries[SRC_A]["migration"]["round_id"] == "round-1"

    # 核对一致
    assert ckpt.verify_positions([SRC_A, SRC_B])["consistent"]


def test_remigrate_is_idempotent(tmp_path):
    """重复迁移不重复记进度：已确认的来源跳过，历史按自然键去重。"""
    router, ckpt, _ = make_services(tmp_path)
    seed_old_layout(router)

    ckpt.migrate_to_versioned_store([SRC_A, SRC_B], round_id="round-1")
    again = ckpt.migrate_to_versioned_store([SRC_A, SRC_B], round_id="round-1")
    assert all(r["status"] == "already_confirmed" for r in again["results"])
    assert router.db.history_count(SRC_A) == 2
    assert router.db.history_count(SRC_B) == 3
    # 换一个轮次号重跑依然幂等
    ckpt.migrate_to_versioned_store([SRC_A, SRC_B], round_id="round-2")
    assert router.db.history_count(SRC_A) == 2
    assert router.db.history_count(SRC_B) == 3


def test_migrate_source_with_history_but_no_json_checkpoint(tmp_path):
    """只有历史记录、没有 JSON 位点的来源：按最后一条历史推导位点。"""
    router, ckpt, _ = make_services(tmp_path)
    router.append_json_history([
        {"source_key": SRC_A, "offset": 700, "line_number": 7, "at": 100.0, "action": "save"},
    ])
    ckpt.migrate_to_versioned_store([SRC_A], round_id="round-1")
    state = router.db.get_checkpoint(SRC_A)
    assert state["offset"] == 700
    assert state["line_number"] == 7
    assert router.db.history_count(SRC_A) == 1


# -- 中断与续传 --

def test_interrupted_migration_resumes(tmp_path):
    """部署中断后可恢复：A 已确认，B 待续传，重跑完成且不重复。"""
    router, ckpt, _ = make_services(tmp_path)
    seed_old_layout(router)

    with pytest.raises(MigrationInterrupted) as exc_info:
        ckpt.migrate_to_versioned_store([SRC_A, SRC_B], round_id="round-1", fail_after=1)
    assert exc_info.value.remaining == [SRC_B]

    # 中断现场：A 在新库，B 还在旧布局
    assert router.storage_for(SRC_A) == "versioned_db"
    assert router.storage_for(SRC_B) == "json_file"

    # 断点续传
    resumed = ckpt.migrate_to_versioned_store([SRC_A, SRC_B], round_id="round-1")
    statuses = {r["source_key"]: r["status"] for r in resumed["results"]}
    assert statuses[SRC_A] == "already_confirmed"
    assert statuses[SRC_B] == "confirmed"
    assert router.db.history_count(SRC_A) == 2
    assert router.db.history_count(SRC_B) == 3


def test_mixed_layout_read_write_and_manifest(tmp_path):
    """同一轮迁移里 A 已写新布局、B 还在追加旧记录，清单同时读两个源。"""
    router, ckpt, fmt = make_services(tmp_path)
    seed_old_layout(router)

    with pytest.raises(MigrationInterrupted):
        ckpt.migrate_to_versioned_store([SRC_A, SRC_B], round_id="round-1", fail_after=1)

    # A 写新布局，B 追加旧记录
    ckpt.save_checkpoint(SRC_A, 1500, 15)
    ckpt.save_checkpoint(SRC_B, 2500, 25)

    # 各就各位：A 进新库，B 进旧 JSON + JSONL
    assert router.db.get_checkpoint(SRC_A)["offset"] == 1500
    assert router.read_json_checkpoint(SRC_A)["offset"] == 1000  # 旧文件冻结在迁移时
    assert router.read_json_checkpoint(SRC_B)["offset"] == 2500
    assert router.db.history_count(SRC_A) == 3  # 2 迁移 + 1 新布局写入
    b_jsonl = [r for r in router.read_json_history() if r["source_key"] == SRC_B]
    assert len(b_jsonl) == 4  # 3 原始 + 1 旧布局追加

    # 成品清单同时读取两个源的位置
    mixed = fmt.get_delivery_manifest([SRC_A, SRC_B])
    assert mixed["on_versioned_db"] == 1
    entries = manifest_positions(mixed)
    assert entries[SRC_A]["position_storage"] == "versioned_db"
    assert entries[SRC_A]["delivered_range"] == {"start_line": 0, "end_line": 15, "offset": 1500}
    assert entries[SRC_B]["position_storage"] == "json_file"
    assert entries[SRC_B]["delivered_range"] == {"start_line": 0, "end_line": 25, "offset": 2500}

    # 续传 B：混合期写入的旧记录不丢，一并迁入
    ckpt.migrate_to_versioned_store([SRC_A, SRC_B], round_id="round-1")
    assert router.db.get_checkpoint(SRC_B)["offset"] == 2500
    assert router.db.history_count(SRC_B) == 4
    rec_b = ckpt.migration_records(SRC_B)[0]
    assert rec_b["original"]["offset"] == 2500  # 存量原值含混合期进度
    assert rec_b["confirmed"] == {"offset": 2500, "line_number": 25}


# -- 完整场景：迁移 -> 中断 -> 读写 -> 回退 -> 核对 --

def test_full_roundtrip_migrate_interrupt_read_write_rollback(tmp_path):
    """迁移两个有历史记录的来源，中断后再次读取并写入，然后回退。

    验证各自确认位置和成品清单没有丢行或重复记进度。
    """
    router, ckpt, fmt = make_services(tmp_path)
    seed_old_layout(router)

    # 1. 同一轮迁移两个来源，处理中断（A 已确认，B 未迁移）
    with pytest.raises(MigrationInterrupted):
        ckpt.migrate_to_versioned_store([SRC_A, SRC_B], round_id="round-1", fail_after=1)

    # 2. 中断后再次读取并写入：A 走新布局，B 走旧布局
    ckpt.save_checkpoint(SRC_A, 1500, 15)
    ckpt.save_checkpoint(SRC_B, 2500, 25)

    # 3. 恢复续传，两个来源都确认进新库
    ckpt.migrate_to_versioned_store([SRC_A, SRC_B], round_id="round-1")

    # 4. 迁移后再次读取并写入（两个源都写新布局）
    ckpt.save_checkpoint(SRC_A, 1800, 18)
    ckpt.save_checkpoint(SRC_B, 3000, 30)

    after = fmt.get_delivery_manifest([SRC_A, SRC_B])
    entries = manifest_positions(after)
    assert entries[SRC_A]["delivered_range"]["offset"] == 1800
    assert entries[SRC_B]["delivered_range"]["offset"] == 3000
    assert ckpt.verify_positions([SRC_A, SRC_B])["consistent"]

    # 5. 回退整个迁移轮次
    rolled = ckpt.rollback_migration(round_id="round-1")
    assert rolled["rolled_back"] == 2

    # 6. 旧读取路径接续迁移期间已确认的采集进度
    assert router.read_json_checkpoint(SRC_A)["offset"] == 1800
    assert router.read_json_checkpoint(SRC_A)["line_number"] == 18
    assert router.read_json_checkpoint(SRC_B)["offset"] == 3000
    assert router.read_json_checkpoint(SRC_B)["line_number"] == 30
    assert router.storage_for(SRC_A) == "json_file"
    assert router.storage_for(SRC_B) == "json_file"

    # 7. 成品清单仍核对到各自确认位置，不丢来源
    final = fmt.get_delivery_manifest([SRC_A, SRC_B])
    assert final["total"] == 2
    assert final["on_versioned_db"] == 0
    entries = manifest_positions(final)
    assert entries[SRC_A]["delivered_range"] == {"start_line": 0, "end_line": 18, "offset": 1800}
    assert entries[SRC_B]["delivered_range"] == {"start_line": 0, "end_line": 30, "offset": 3000}
    assert entries[SRC_A]["migration"]["status"] == "rolled_back"

    # 8. 历史不丢行：新库确认的记录回退后都能旧 JSONL 找到
    jsonl_a = [r for r in router.read_json_history() if r["source_key"] == SRC_A]
    jsonl_b = [r for r in router.read_json_history() if r["source_key"] == SRC_B]
    assert len(jsonl_a) == 2 + 2 + 1  # 原始 + 新布局期间回填 + 回退记录
    assert len(jsonl_b) == 4 + 1 + 1
    assert router.db.history_count(SRC_A) == 4  # 新库历史保留可审计
    assert router.db.history_count(SRC_B) == 5

    # 9. 迁移记录保留：原值、确认范围、回退落点全程可核对
    rec_a = ckpt.migration_records(SRC_A)[0]
    assert rec_a["status"] == "rolled_back"
    assert rec_a["original"]["offset"] == 1000
    assert rec_a["confirmed"] == {"offset": 1000, "line_number": 10}
    assert rec_a["rollback_position"]["offset"] == 1800

    # 10. 不重复记进度：最终核对一致
    verify = ckpt.verify_positions([SRC_A, SRC_B])
    assert verify["consistent"], verify
    assert verify["sources"][SRC_A]["history"]["json_file"] == 5
    assert verify["sources"][SRC_B]["history"]["json_file"] == 6


def test_rollback_then_continue_on_old_path(tmp_path):
    """回退后旧读取路径继续工作：写在旧布局，不回头污染新库。"""
    router, ckpt, fmt = make_services(tmp_path)
    seed_old_layout(router)

    ckpt.migrate_to_versioned_store([SRC_A], round_id="round-1")
    ckpt.save_checkpoint(SRC_A, 1800, 18)
    ckpt.rollback_migration(round_id="round-1")

    # 旧路径接续写入
    ckpt.save_checkpoint(SRC_A, 2000, 20)
    assert router.read_json_checkpoint(SRC_A)["offset"] == 2000
    # 新库位点停留在回退时确认的进度
    assert router.db.get_checkpoint(SRC_A)["offset"] == 1800
    entry = manifest_positions(fmt.get_delivery_manifest([SRC_A]))[SRC_A]
    assert entry["position_storage"] == "json_file"
    assert entry["delivered_range"]["offset"] == 2000


# -- 存储上线后，采集与格式输出核对真实位置 --

def test_collection_and_manifest_use_versioned_store(tmp_path):
    """采集从新库位点继续，格式输出清单核对到同一真实位置。"""
    router, ckpt, fmt = make_services(tmp_path)

    sources = [s for s in store.rows("log_source") if s.get("status") == "已接入"]
    if not sources:
        pytest.skip("无已接入日志源")
    source = sources[0]
    key = source["源编号"]

    # 旧布局位点 -> 迁移上线
    router.write_json_checkpoint(key, {
        "source_key": key, "offset": 500, "line_number": 5, "status": "active",
    })
    ckpt.migrate_to_versioned_store([key], round_id="round-live")

    # 采集接续新库位点（日志行写到平台数据目录，位点只走隔离存储）
    log_svc = LogSourceService(checkpoint_router=router)
    result = log_svc.collect_from_source(source["id"], batch_size=3)
    assert result["start_line"] == 5
    assert result["end_line"] == 8

    # 新库位点推进，旧 JSON 冻结在迁移时的值
    assert router.db.get_checkpoint(key)["line_number"] == 8
    assert router.read_json_checkpoint(key)["line_number"] == 5

    # 格式输出清单从新库核对到同一位置
    entry = manifest_positions(fmt.get_delivery_manifest([key]))[key]
    assert entry["position_storage"] == "versioned_db"
    assert entry["delivered_range"]["end_line"] == 8

    # 回退后旧读取路径接续已确认进度，清单核对一致
    ckpt.rollback_migration(round_id="round-live")
    assert router.read_json_checkpoint(key)["line_number"] == 8
    entry = manifest_positions(fmt.get_delivery_manifest([key]))[key]
    assert entry["position_storage"] == "json_file"
    assert entry["delivered_range"]["end_line"] == 8

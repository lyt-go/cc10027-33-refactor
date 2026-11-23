"""代理采集位置解析契约与管道/离线回滚的回归测试。

覆盖两类场景：
1. 明确归属（含旧代理名与现行源编号不同的别名）在保存进度与离线回滚中命中同一个源；
2. 历史代理名同时可能对应多个源时的歧义数据不被静默归并，原输入可追查。
同时验证确认凭据的幂等性与失败重试。
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from app.services import agent_source_resolution as resolution
from app.services.agent_heartbeat import AgentHeartbeatService
from app.services.agent_source_resolution import (
    ATTRIBUTION_AMBIGUOUS,
    ATTRIBUTION_CONFIRMED,
    ATTRIBUTION_DERIVED,
    ConfirmationLedger,
    agent_source_resolver,
    confirmation_ledger,
)
from app.services.collect_checkpoint import CollectCheckpointService
from app.services.pipeline import CrossModulePipeline
from app.store import store

DATA_CHECKPOINTS = resolution.checkpoint_dir()


@pytest.fixture(autouse=True)
def clean_state():
    """每个用例使用独立的别名归属、账本与位点目录。"""
    agent_source_resolver.clear_aliases()
    confirmation_ledger.reset()
    audit_path = DATA_CHECKPOINTS / resolution.AUDIT_FILE
    if audit_path.exists():
        audit_path.unlink()
    yield
    agent_source_resolver.clear_aliases()
    confirmation_ledger.reset()


def _register_agent(agent_code: str) -> None:
    rows = store.rows("agent_heartbeat")
    if any(row.get("代理编号") == agent_code for row in rows):
        return
    rows.append({
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "代理编号": agent_code,
        "部署主机": f"host-{agent_code}",
        "代理版本": "v2.1",
        "上次心跳": "",
        "采集量": "0",
        "错误率": "0",
        "内存占用": "0",
        "status": "运行中",
        "pending": False,
        "abnormal": False,
    })


def _fresh_source(source_key: str, offset: int, line_number: int = 7) -> Path:
    """写一个可辨认的历史位点，用来确认归并/回滚没有改错源。"""
    svc = CollectCheckpointService()
    svc.save_checkpoint(source_key, offset, line_number)
    path = DATA_CHECKPOINTS / f"{source_key}.json"
    assert path.exists()
    return path


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------
# 1. 明确归属：旧代理名 / 现行源编号不同
# --------------------------------------------------------------------------

def test_legacy_agent_name_with_alias_attributes_to_current_source():
    """旧代理名 AG-T-OLD 属于现行源 LOG_-0003，而不是按其名字猜测的源。"""
    agent_source_resolver.register_alias("AG-T-OLD", "LOG_-0003", status="已确认")

    attribution = agent_source_resolver.resolve("AG-T-OLD", operation="save")

    assert attribution.status == ATTRIBUTION_CONFIRMED
    assert attribution.reason == "alias-registry"
    assert attribution.source_key == "LOG_-0003"
    assert attribution.candidates == ("LOG_-0003",)
    assert not attribution.ambiguous


def test_missing_prefix_is_not_misread_when_alias_exists():
    """代理名不带 AGENT- 前缀时，有别名归属就按别名，不再退化成名字推导。"""
    agent_source_resolver.register_alias("worker-7", "LOG_-0002")

    by_alias = agent_source_resolver.resolve("worker-7", operation="save")
    by_prefix = agent_source_resolver.resolve("worker-7", operation="save", requested_source=None)

    assert by_alias.source_key == "LOG_-0002"
    assert by_prefix.source_key == "LOG_-0002"
    assert by_prefix.reason == "alias-registry"


def test_registering_same_alias_twice_keeps_single_candidate():
    agent_source_resolver.register_alias("AG-T-OLD", "LOG_-0003")
    first = store.rows(resolution.ALIAS_MODULE)[0]["id"]
    agent_source_resolver.register_alias("AG-T-OLD", "LOG_-0003", status="已确认")
    rows = store.rows(resolution.ALIAS_MODULE)

    assert len(rows) == 1
    assert rows[0]["id"] == first
    assert agent_source_resolver.resolve("AG-T-OLD").source_key == "LOG_-0003"


# --------------------------------------------------------------------------
# 2. 歧义：历史代理名均有可能对应
# --------------------------------------------------------------------------

def test_aliases_pointing_to_two_sources_are_ambiguous_and_audited():
    agent_source_resolver.register_alias("AG-T-AMB", "LOG_-0001")
    agent_source_resolver.register_alias("AG-T-AMB", "LOG_-0004")

    before_1 = _fresh_source("LOG_-0001", 111, 1)
    before_4 = _fresh_source("LOG_-0004", 444, 4)
    raw_input = {"agent_code": "AG-T-AMB", "payload": "received-as-is", "seq": 9}

    attribution = agent_source_resolver.resolve(
        "AG-T-AMB", operation="rollback", raw_input=raw_input,
    )

    assert attribution.status == ATTRIBUTION_AMBIGUOUS
    assert attribution.ambiguous
    assert attribution.source_key is None
    assert set(attribution.candidates) == {"LOG_-0001", "LOG_-0004"}
    assert attribution.reason == "alias-ownership-conflict"

    # 歧义不会改写任何一个历史位点
    assert _read(before_1)["offset"] == 111
    assert _read(before_4)["offset"] == 444

    trace = agent_source_resolver.trace(agent_code="AG-T-AMB", ambiguous_only=True)
    assert len(trace) == 1
    assert trace[0]["raw_input"]["payload"] == "received-as-is"
    assert trace[0]["raw_input"]["seq"] == 9
    assert trace[0]["candidates"] == ["LOG_-0001", "LOG_-0004"]


def test_inactive_alias_does_not_create_conflict():
    agent_source_resolver.register_alias("AG-T-OLD", "LOG_-0001", status="已失效")
    agent_source_resolver.register_alias("AG-T-OLD", "LOG_-0003", status="已确认")

    attribution = agent_source_resolver.resolve("AG-T-OLD")

    assert attribution.status == ATTRIBUTION_CONFIRMED
    assert attribution.source_key == "LOG_-0003"
    assert attribution.candidates == ("LOG_-0003",)


def test_agent_registration_conflict_is_ambiguous():
    """代理登记行里显式关联了两个源时同样不猜。"""
    row = store.rows("agent_heartbeat")[0]
    row["代理编号"] = "AG-CONFLICT"
    store.rows("agent_heartbeat")[1].update({
        "代理编号": "AG-CONFLICT",
        "关联源": "LOG_-0001",
    })
    row["关联源"] = "LOG_-0002"

    attribution = agent_source_resolver.resolve("AG-CONFLICT")

    assert attribution.ambiguous
    assert set(attribution.candidates) == {"LOG_-0001", "LOG_-0002"}

    row.pop("关联源", None)
    store.rows("agent_heartbeat")[1].pop("关联源", None)


# --------------------------------------------------------------------------
# 3. 管道：登记实际源位点 / 离线回滚按同一归属
# --------------------------------------------------------------------------

def test_pipeline_records_position_on_resolved_source():
    agent_source_resolver.register_alias("AG-T-OLD", "LOG_-0003")
    _register_agent("AG-T-OLD")
    pipeline = CrossModulePipeline()

    result = pipeline.agent_checkpoint_pipeline("AG-T-OLD")

    position = result["position_result"]
    assert position["source_key"] == "LOG_-0003"
    assert position["offset"] == 4096
    assert position["checkpoint_file"] == "LOG_-0003.json"
    # 位置结果写入实际源；心跳结果仍在 agent_<编号>.json，两者分开可查
    assert _read(DATA_CHECKPOINTS / "LOG_-0003.json")["offset"] == 4096
    assert _read(DATA_CHECKPOINTS / "agent_AG-T-OLD.json")["status"] == "运行中"
    assert result["heartbeat_result"]["checkpoint_file"] == "agent_AG-T-OLD.json"


def test_pipeline_ambiguous_agent_writes_no_position():
    agent_source_resolver.register_alias("AG-T-AMB", "LOG_-0001")
    agent_source_resolver.register_alias("AG-T-AMB", "LOG_-0004")
    _register_agent("AG-T-AMB")
    _fresh_source("LOG_-0001", 10, 1)
    _fresh_source("LOG_-0004", 40, 4)

    result = CrossModulePipeline().agent_checkpoint_pipeline("AG-T-AMB")

    assert result["position_result"]["status"] == "ambiguous"
    assert result["position_result"]["candidates"]
    assert _read(DATA_CHECKPOINTS / "LOG_-0001.json")["offset"] == 10
    assert _read(DATA_CHECKPOINTS / "LOG_-0004.json")["offset"] == 40
    # 心跳照样记在代理自己的命名空间
    assert (DATA_CHECKPOINTS / "agent_AG-T-AMB.json").exists()


def test_offline_rollback_uses_same_attribution_as_save():
    agent_source_resolver.register_alias("AG-T-OLD", "LOG_-0003")
    svc = CollectCheckpointService()
    saved = svc.record_agent_position("AG-T-OLD", 4096, 200, operation="heartbeat-progress")
    assert saved["source_key"] == "LOG_-0003"

    rolled = svc.rollback_agent_position("AG-T-OLD", 0, operation="offline-rollback")

    assert rolled["source_key"] == "LOG_-0003"
    assert rolled["offset"] == 0
    assert rolled["status"] == "rolled_back"
    state = _read(DATA_CHECKPOINTS / "LOG_-0003.json")
    assert state["offset"] == 0
    assert state["offset_before_rollback"] == 4096


def test_offline_rollback_ambiguous_leaves_every_source_readable():
    agent_source_resolver.register_alias("AG-T-AMB", "LOG_-0001")
    agent_source_resolver.register_alias("AG-T-AMB", "LOG_-0004")
    path_1 = _fresh_source("LOG_-0001", 111, 1)
    path_4 = _fresh_source("LOG_-0004", 444, 4)

    result = CollectCheckpointService().rollback_agent_position("AG-T-AMB", 0)

    assert result["status"] == "ambiguous"
    assert "未回滚" in result["error"]
    assert _read(path_1)["offset"] == 111
    assert _read(path_4)["offset"] == 444


def test_same_agent_save_then_offline_rollback_hits_same_source():
    """同一代理：先提交心跳进度，随后离线检测按同一归属回滚同一位点。"""
    agent_source_resolver.register_alias("AG-T-OLD", "LOG_-0003")
    _register_agent("AG-T-OLD")
    pipeline = CrossModulePipeline()

    saved = pipeline.agent_checkpoint_pipeline("AG-T-OLD")
    assert saved["position_result"]["source_key"] == "LOG_-0003"
    assert _read(DATA_CHECKPOINTS / "LOG_-0003.json")["offset"] == 4096

    # 让该代理超时，触发离线检测
    row = next(r for r in store.rows("agent_heartbeat") if r.get("代理编号") == "AG-T-OLD")
    row["_last_heartbeat_ts"] = time.time() - AgentHeartbeatService.HEARTBEAT_TIMEOUT - 60
    row["status"] = "运行中"

    offline = AgentHeartbeatService().detect_offline_agents()

    item = next(i for i in offline if i["代理编号"] == "AG-T-OLD")
    assert item["source_key"] == "LOG_-0003"
    assert "auto_rollback" in item
    assert item["auto_rollback"]["source_key"] == "LOG_-0003"
    state = _read(DATA_CHECKPOINTS / "LOG_-0003.json")
    assert state["offset"] == 0
    assert state["offset_before_rollback"] == 4096
    # 心跳结果仍单独留在代理命名空间里
    assert _read(DATA_CHECKPOINTS / "agent_AG-T-OLD.json")["status"] == "已离线"


def test_same_ambiguous_agent_does_not_roll_back_either_source():
    agent_source_resolver.register_alias("AG-T-AMB2", "LOG_-0001")
    agent_source_resolver.register_alias("AG-T-AMB2", "LOG_-0004")
    _register_agent("AG-T-AMB2")
    _fresh_source("LOG_-0001", 111, 1)
    _fresh_source("LOG_-0004", 444, 4)

    pipeline = CrossModulePipeline()
    saved = pipeline.agent_checkpoint_pipeline("AG-T-AMB2")
    assert saved["position_result"]["status"] == "ambiguous"

    row = next(r for r in store.rows("agent_heartbeat") if r.get("代理编号") == "AG-T-AMB2")
    row["_last_heartbeat_ts"] = time.time() - AgentHeartbeatService.HEARTBEAT_TIMEOUT - 60
    row["status"] = "运行中"

    item = AgentHeartbeatService().detect_offline_agents()[0]
    skipped = item["rollback_skipped"]
    assert skipped["status"] == "ambiguous"
    # 离线回滚的原输入也能追回
    assert skipped["trace"]["raw_input"]["trigger"] == "offline-detection"

    assert _read(DATA_CHECKPOINTS / "LOG_-0001.json")["offset"] == 111
    assert _read(DATA_CHECKPOINTS / "LOG_-0004.json")["offset"] == 444
    trace = agent_source_resolver.trace(agent_code="AG-T-AMB2", ambiguous_only=True)
    triggers = {t["raw_input"].get("trigger") or t["raw_input"].get("context", {}).get("source")
                for t in trace}
    assert "offline-detection" in triggers


# --------------------------------------------------------------------------
# 4. 确认凭据幂等 / 失败恢复
# --------------------------------------------------------------------------

def test_same_confirmation_token_updates_position_once():
    svc = CollectCheckpointService()
    first = svc.record_agent_position(
        "AGENT-LOG_-0003", 4096, 200,
        operation="heartbeat-progress",
        confirmation_token="AC-fixed-1",
    )
    # 模拟重复投递：第二次携带同一个凭据
    svc._apply_position("LOG_-0003", 8192, 400)  # 外部又推进了一次位点
    second = svc.record_agent_position(
        "AGENT-LOG_-0003", 4096, 200,
        operation="heartbeat-progress",
        confirmation_token="AC-fixed-1",
    )

    assert first["duplicate"] is False
    assert second["duplicate"] is True
    assert second["status"] == "applied"
    # 位点保持第二次外部推进的值，凭据没有再次覆盖
    assert _read(DATA_CHECKPOINTS / "LOG_-0003.json")["offset"] == 8192
    history = [
        line for line in (DATA_CHECKPOINTS / "checkpoint_history.jsonl").read_text().splitlines()
        if "AC-fixed-1" in line
    ]
    # 首次受理写一条；重复投递不再追加位置更新记录
    assert len(history) == 1


def test_token_cannot_be_reused_for_different_source():
    svc = CollectCheckpointService()
    svc.record_agent_position(
        "AGENT-LOG_-0003", 4096, 200,
        operation="heartbeat-progress", confirmation_token="AC-cross",
    )
    agent_source_resolver.register_alias("AGENT-LOG_-0003", "LOG_-0004")

    result = svc.record_agent_position(
        "AGENT-LOG_-0003", 4096, 200,
        operation="heartbeat-progress", confirmation_token="AC-cross",
    )

    assert result["status"] == "conflict"
    assert "拒绝重复更新" in result["error"]
    assert _read(DATA_CHECKPOINTS / "LOG_-0004.json").get("offset", 0) != 4096


def test_failed_write_can_recover_with_same_token(monkeypatch):
    svc = CollectCheckpointService()
    calls = {"count": 0}
    real_apply = svc._apply_position

    def flaky_apply(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("磁盘暂时不可用")
        return real_apply(*args, **kwargs)

    monkeypatch.setattr(svc, "_apply_position", flaky_apply)

    failed = svc.record_agent_position(
        "AGENT-LOG_-0003", 4096, 200,
        operation="heartbeat-progress", confirmation_token="AC-recover",
    )
    assert failed["status"] == "failed"
    assert "重试" in failed["error"]

    recovered = svc.record_agent_position(
        "AGENT-LOG_-0003", 4096, 200,
        operation="heartbeat-progress", confirmation_token="AC-recover",
    )
    assert recovered["status"] == "active"
    assert recovered["duplicate"] is False
    assert _read(DATA_CHECKPOINTS / "LOG_-0003.json")["offset"] == 4096

    # 恢复成功之后再投递同一凭据不会重复写
    again = svc.record_agent_position(
        "AGENT-LOG_-0003", 4096, 200,
        operation="heartbeat-progress", confirmation_token="AC-recover",
    )
    assert again["duplicate"] is True
    assert calls["count"] == 2


def test_ledger_persists_across_instances():
    first_ledger = ConfirmationLedger(filename="ledger_persist_test.json")
    first_ledger.reset()
    allowed, _ = first_ledger.claim(
        "AC-persist", operation="save", agent_code="AGENT-X", source_key="LOG_-0003",
    )
    assert allowed
    first_ledger.complete("AC-persist", {"offset": 1})

    second_ledger = ConfirmationLedger(filename="ledger_persist_test.json")
    allowed_again, record = second_ledger.claim(
        "AC-persist", operation="save", agent_code="AGENT-X", source_key="LOG_-0003",
    )
    assert not allowed_again
    assert record["status"] == "applied"
    second_ledger.reset()


# --------------------------------------------------------------------------
# 5. 重构前后对照：已验证输入的结果不变
# --------------------------------------------------------------------------

def test_confirmed_inputs_match_pre_refactor_behaviour():
    """重构前的解析方式：AGENT- 前缀直接去掉得到源编号。"""
    svc = CollectCheckpointService()

    for agent_code in ("AGENT-LOG_-0003", "AGENT-LOG_-0001"):
        legacy_source = agent_code.replace("AGENT-", "")
        attribution = svc.record_agent_position(agent_code, 512, 12, operation="heartbeat-progress")
        assert attribution["source_key"] == legacy_source
        assert attribution["attribution"]["status"] == ATTRIBUTION_DERIVED
        assert _read(DATA_CHECKPOINTS / f"{legacy_source}.json")["offset"] == 512

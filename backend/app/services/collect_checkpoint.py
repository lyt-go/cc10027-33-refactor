"""采集位点管理：采集进度记录、位点提交、回滚和过期管理。"""
from __future__ import annotations

import time
from typing import Any

from app.services.agent_source_resolution import (
    confirmation_ledger,
    resolve_agent_source,
)
from app.services.base import BaseModuleService


class CollectCheckpointService(BaseModuleService):
    MODULE = "collect_checkpoint"
    ENTITY = "位点记录"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["记录编号", "关联源", "文件偏移", "处理行号", "位点时间", "校验摘要", "提交状态", "回滚位点"]
    REQUIRED_FIELDS = ["记录编号", "关联源", "文件偏移"]
    STATUS_ORDER = ["采集中", "已提交", "已回滚", "已过期"]
    ACTION_RULES = {"确认提交": "已提交", "发起回滚": "已回滚", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []

    def save_checkpoint(self, source_key: str, offset: int, line_number: int) -> dict[str, Any]:
        """保存采集位点状态。"""
        state = self._apply_position(source_key, offset, line_number)
        return state

    def _apply_position(
        self,
        source_key: str,
        offset: int,
        line_number: int | None = None,
        *,
        action: str = "save",
        agent_code: str | None = None,
        confirmation_token: str | None = None,
    ) -> dict[str, Any]:
        """真正写入位点文件；代理相关调用只在账本允许时才会到这里。

        写入时保留位点文件里已有字段（如校验摘要、采集时间、既有 previous_offset），
        避免归并同名代理时把其他源的历史信息冲掉。
        """
        now = time.time()
        previous = self._read_checkpoint(source_key) or {}
        state = dict(previous)
        state["source_key"] = source_key
        state["offset"] = offset
        if line_number is None:
            state.setdefault("line_number", 0)
        else:
            state["line_number"] = line_number
        state["updated_at"] = now

        if action == "save":
            state["status"] = "active"
        else:
            state["status"] = "rolled_back"
            state["rolled_back_at"] = now
            # 保持原实现语义：已有 previous_offset 沿用，否则取本次回滚目标值。
            state["previous_offset"] = previous.get("previous_offset", offset)
            # 额外留一份回滚前的实际偏移，便于回查（不改变 previous_offset 含义）。
            state["offset_before_rollback"] = previous.get("offset", 0)
        self._write_checkpoint(source_key, state)

        history = {
            "source_key": source_key, "offset": offset,
            "line_number": state["line_number"], "at": now, "action": action,
        }
        if agent_code:
            history["agent_code"] = agent_code
        if confirmation_token:
            history["confirmation_token"] = confirmation_token
        self._write_jsonl(self._data_dir("checkpoints") / "checkpoint_history.jsonl", [history])
        return state

    # -- 代理采集位置（统一走解析契约 + 确认凭据账本） --

    def _resolve_agent_operation(
        self,
        agent_code: str,
        *,
        operation: str,
        requested_source: str | None,
        confirmation_token: str | None,
        raw_input: dict[str, Any] | None,
        context: dict[str, Any] | None,
        audit: bool | None,
        event_key: str = "",
    ):
        attribution = resolve_agent_source(
            agent_code,
            operation=operation,
            requested_source=requested_source,
            confirmation_token=confirmation_token,
            event_key=event_key,
            raw_input=raw_input,
            context=context,
            audit=audit,
        )
        if attribution.ambiguous:
            return attribution, {
                "error": f"代理 {agent_code} 归属歧义，未更新任何源位点",
                "status": "ambiguous",
                "candidates": list(attribution.candidates),
                "confirmation_token": attribution.confirmation_token,
                "input_digest": attribution.input_digest,
                "trace": attribution.trace_record(),
            }
        if attribution.source_key is None:
            return attribution, {
                "error": f"代理 {agent_code} 未关联到日志源，未更新位点",
                "status": "unresolved",
                "confirmation_token": attribution.confirmation_token,
                "input_digest": attribution.input_digest,
                "trace": attribution.trace_record(),
            }
        return attribution, None

    def record_agent_position(
        self,
        agent_code: str,
        offset: int,
        line_number: int = 0,
        *,
        operation: str = "save",
        requested_source: str | None = None,
        confirmation_token: str | None = None,
        raw_input: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
        audit: bool | None = None,
        event_key: str = "",
    ) -> dict[str, Any]:
        """按解析契约把代理进度登记到实际日志源位点。

        位点命名空间仍是 ``<源编号>.json``，代理心跳文件 ``agent_<代理编号>.json``
        不被触碰；同一个确认凭据最多真正写入一次，失败后可用同一凭据重试。
        """
        attribution, rejection = self._resolve_agent_operation(
            agent_code,
            operation=operation,
            requested_source=requested_source,
            confirmation_token=confirmation_token,
            raw_input=raw_input,
            context=context,
            audit=audit,
            event_key=event_key,
        )
        if rejection is not None:
            return rejection
        return self._apply_with_ledger(
            attribution,
            operation=f"{operation}-position",
            offset=offset,
            line_number=line_number,
            action="save",
        )

    def rollback_agent_position(
        self,
        agent_code: str,
        to_offset: int = 0,
        *,
        operation: str = "rollback",
        requested_source: str | None = None,
        confirmation_token: str | None = None,
        raw_input: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
        audit: bool | None = None,
        event_key: str = "",
    ) -> dict[str, Any]:
        """按与保存时相同的归属回滚代理关联源的位点。"""
        attribution, rejection = self._resolve_agent_operation(
            agent_code,
            operation=operation,
            requested_source=requested_source,
            confirmation_token=confirmation_token,
            raw_input=raw_input,
            context=context,
            audit=audit,
            event_key=event_key,
        )
        if rejection is not None:
            if rejection.get("status") in ("ambiguous", "unresolved"):
                rejection["error"] = rejection["error"].replace("未更新任何源位点", "未回滚任何源位点")
            return rejection
        return self._apply_with_ledger(
            attribution,
            operation=f"{operation}-position",
            offset=to_offset,
            line_number=None,
            action="rollback",
        )

    def _apply_with_ledger(
        self,
        attribution,
        *,
        operation: str,
        offset: int,
        line_number: int | None,
        action: str,
    ) -> dict[str, Any]:
        token = attribution.confirmation_token
        allowed, ledger_record = confirmation_ledger.claim(
            token,
            operation=operation,
            agent_code=attribution.agent_code,
            source_key=attribution.source_key or "",
        )
        if not allowed:
            if "error" in ledger_record:
                return {
                    **ledger_record,
                    "status": "conflict",
                    "attribution": attribution.as_dict(),
                }
            return {
                **(ledger_record.get("result") or {}),
                "status": "applied",
                "duplicate": True,
                "confirmation_token": token,
                "attribution": attribution.as_dict(),
            }

        try:
            state = self._apply_position(
                attribution.source_key or "",
                offset,
                line_number,
                action=action,
                agent_code=attribution.agent_code,
                confirmation_token=token,
            )
        except Exception as exc:  # noqa: BLE001 - 失败要允许同一凭据恢复后重试
            confirmation_ledger.release(token, str(exc))
            return {
                "error": f"位点更新失败，可用同一确认凭据重试: {exc}",
                "status": "failed",
                "confirmation_token": token,
                "attribution": attribution.as_dict(),
            }

        result = {
            **state,
            "attribution": attribution.as_dict(),
            "confirmation_token": token,
            "duplicate": False,
        }
        confirmation_ledger.complete(token, result)
        return result

    def rollback_to(self, source_key: str, to_offset: int) -> dict[str, Any]:
        """将指定位点回滚到指定偏移位置。"""
        cp = self._read_checkpoint(source_key)
        if cp is None:
            return {"error": f"位点 {source_key} 不存在"}

        return self._apply_position(source_key, to_offset, action="rollback")

    def get_checkpoint_state(self, source_key: str) -> dict[str, Any]:
        """获取指定源的位点信息。"""
        cp = self._read_checkpoint(source_key)
        if cp is None:
            return {"source_key": source_key, "status": "no_checkpoint", "offset": 0}
        return cp

    def list_all_checkpoints(self) -> list[dict[str, Any]]:
        """列出所有位点状态。"""
        all_cps = []
        for src in self._collect_sources():
            key = src["源编号"]
            cp = self._read_checkpoint(key)
            if cp:
                all_cps.append(cp)
        return all_cps

    def commit_checkpoint(self, source_key: str) -> dict[str, Any]:
        """提交采集位点并自动触发下游格式化输出。"""
        cp = self._read_checkpoint(source_key)
        if cp is None:
            return {"error": f"位点 {source_key} 不存在"}
        cp["status"] = "committed"
        cp["committed_at"] = time.time()
        self._write_checkpoint(source_key, cp)

        # 写入位点历史
        self._write_jsonl(self._data_dir("checkpoints") / "checkpoint_history.jsonl", [{
            "source_key": source_key, "offset": cp.get("offset", 0),
            "at": time.time(), "action": "commit",
        }])

        result = {
            "source_key": source_key,
            "committed_at": time.time(),
            "offset": cp.get("offset", 0),
            "line_number": cp.get("line_number", 0),
        }

        # 提交后自动触发格式输出
        export_r = self._trigger_export_for_source(source_key)
        if "error" not in export_r:
            result["auto_export"] = export_r

        # 同时触发质量诊断
        quality_r = self._trigger_quality_diagnosis(source_key)
        if "error" not in quality_r:
            result["auto_quality"] = quality_r

        return result
        """验证采集位点的校验摘要。"""
        cp = self._read_checkpoint(source_key)
        if cp is None:
            return {"source_key": source_key, "status": "no_checkpoint", "match": False}

        actual = cp.get("checksum", "")
        match = actual == expected_checksum
        cp["last_verified_at"] = time.time()
        cp["last_verified_match"] = match
        self._write_checkpoint(source_key, cp)

        return {"source_key": source_key, "match": match, "expected": expected_checksum, "actual": actual}
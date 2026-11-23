"""代理采集位置解析契约：把代理编号归属到唯一的日志源位点。

这个契约同时服务于两处历史实现：

* 代理心跳管道提交采集进度时，需要知道进度应该写到哪个日志源位点；
* 代理离线后尝试回滚时，需要按与提交时完全相同的归属找到同一个位点。

解析结果除了 ``source_key`` 之外，还携带收到时的原始输入、候选源、确认凭据
（token）和歧义标记。归属不唯一时不会猜测，也不会改写任何已有位点；原始输入
会写入 ``checkpoints/agent_source_resolution.jsonl`` 供事后追查。

位点命名空间约定保持不变：

* ``agent_<代理编号>.json`` 只保存心跳/代理状态；
* ``<源编号>.json`` 保存采集位置。

两者互不覆盖，因此回查时可以分开看到心跳结果和位置结果。
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.services import base
from app.store import store

ALIAS_MODULE = "agent_source_alias"
AGENT_PREFIX = "AGENT-"
AUDIT_FILE = "agent_source_resolution.jsonl"
LEDGER_FILE = "agent_confirmation_ledger.json"

# 失效的别名状态；不在该集合里的状态（含空值/历史别名）都按生效处理，
# 这样旧数据仍然可读，不会被当成新归属而改写位点。
INACTIVE_ALIAS_STATES = {"已失效", "已归档", "已归并", "已合并", "disabled", "archived", "inactive"}

ATTRIBUTION_CONFIRMED = "confirmed"
ATTRIBUTION_DERIVED = "derived"
ATTRIBUTION_AMBIGUOUS = "ambiguous"
ATTRIBUTION_UNRESOLVED = "unresolved"


def checkpoint_dir() -> Path:
    """与模块服务共用同一个数据根目录，便于测试整体重定向。"""
    path = base.DATA_ROOT / "checkpoints"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _digest(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _new_token(operation: str, agent_code: str, source_key: str | None) -> str:
    """每次新建操作生成新凭据；重试必须回传上一次的凭据。"""
    nonce = f"{time.time_ns()}-{os.getpid()}-{threading.get_ident()}"
    return "AC-" + _digest({
        "operation": operation,
        "agent_code": agent_code,
        "source_key": source_key,
        "nonce": nonce,
    })


def _distinct(values: list[str]) -> tuple[str, ...]:
    seen: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            seen.append(text)
    return tuple(seen)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


@dataclass(frozen=True)
class SourceAttribution:
    """一次代理编号解析的完整结果（不可变，可安全回传/留档）。"""

    agent_code: str
    operation: str
    status: str
    source_key: str | None
    candidates: tuple[str, ...]
    reason: str
    raw_input: dict[str, Any]
    input_digest: str
    confirmation_token: str
    alias_sources: tuple[dict[str, Any], ...] = ()

    @property
    def resolved(self) -> bool:
        return self.source_key is not None and not self.ambiguous

    @property
    def ambiguous(self) -> bool:
        return self.status == ATTRIBUTION_AMBIGUOUS

    def as_dict(self) -> dict[str, Any]:
        """给管道/接口用的精简摘要（不含完整原始输入）。"""
        return {
            "agent_code": self.agent_code,
            "operation": self.operation,
            "status": self.status,
            "source_key": self.source_key,
            "candidates": list(self.candidates),
            "reason": self.reason,
            "ambiguous": self.ambiguous,
            "input_digest": self.input_digest,
            "confirmation_token": self.confirmation_token,
        }

    def trace_record(self) -> dict[str, Any]:
        """落盘追查记录：保留收到时的原输入，不只保留解析后的结论。"""
        record = self.as_dict()
        record["raw_input"] = self.raw_input
        record["alias_sources"] = [dict(item) for item in self.alias_sources]
        record["recorded_at"] = time.time()
        return record


class AgentSourceResolver:
    """代理编号 -> 日志源位点的唯一归属解析器。"""

    def __init__(self, alias_module: str = ALIAS_MODULE) -> None:
        self.alias_module = alias_module

    # -- 别名登记（保留历史代理名，不覆盖其他源位点） --

    def register_alias(
        self,
        agent_code: str,
        source_key: str,
        *,
        status: str = "已确认",
        note: str | None = None,
    ) -> dict[str, Any]:
        """登记一条历史代理名归属。

        同一个 ``代理编号 + 源编号`` 重复登记会原地刷新状态，不会产生重复候选；
        同名代理指向不同源时则保留多条记录，让解析器判定为歧义，避免静默改写。
        """
        agent_code = str(agent_code or "").strip()
        source_key = str(source_key or "").strip()
        if not agent_code or not source_key:
            raise ValueError("代理编号和源编号均不能为空")

        rows = store.rows(self.alias_module)
        for row in rows:
            if row.get("代理编号") == agent_code and row.get("源编号") == source_key:
                row["status"] = status
                row["note"] = note if note is not None else row.get("note")
                row["updated_at"] = time.time()
                return row

        entry = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "代理编号": agent_code,
            "源编号": source_key,
            "status": status,
            "note": note,
            "created_at": time.time(),
        }
        rows.append(entry)
        return entry

    def clear_aliases(self, agent_code: str | None = None) -> int:
        rows = store.rows(self.alias_module)
        if agent_code is None:
            removed = len(rows)
            rows.clear()
            return removed
        kept = [row for row in rows if row.get("代理编号") != agent_code]
        removed = len(rows) - len(kept)
        rows[:] = kept
        return removed

    # -- 归属解析 --

    def _alias_rows(self, agent_code: str) -> list[dict[str, Any]]:
        rows = []
        for row in store.rows(self.alias_module):
            if str(row.get("代理编号", "")).strip() != agent_code:
                continue
            state = str(row.get("status") or row.get("归属状态") or "").strip()
            if state in INACTIVE_ALIAS_STATES:
                continue
            # 未知状态按生效处理，保留历史数据的可读性。
            rows.append(row)
        return rows

    @staticmethod
    def _alias_source(row: dict[str, Any]) -> str:
        return str(row.get("源编号") or row.get("关联源") or "").strip()

    @staticmethod
    def _explicit_bindings(agent_code: str) -> tuple[str, ...]:
        """代理登记行里显式写明的关联源（如果有）。"""
        values: list[str] = []
        for row in store.rows("agent_heartbeat"):
            if str(row.get("代理编号", "")).strip() != agent_code:
                continue
            if str(row.get("status") or "").strip() in INACTIVE_ALIAS_STATES:
                continue
            for field in ("关联源", "源编号", "源位点"):
                values.append(str(row.get(field) or "").strip())
        return _distinct(values)

    @staticmethod
    def confirmation_token(
        operation: str,
        agent_code: str,
        source_key: str | None,
        event_key: str = "",
    ) -> str:
        """生成稳定的确认凭据；同一操作的失败重试应复用同一个 token。"""
        return "AC-" + _digest({
            "operation": operation,
            "agent_code": agent_code,
            "source_key": source_key,
            "event_key": event_key,
        })

    @staticmethod
    def event_token(
        operation: str,
        agent_code: str,
        source_key: str | None,
        event_key: str,
    ) -> str:
        """由上游业务事件推导的稳定凭据（重放同一事件不会重复更新位点）。"""
        return AgentSourceResolver.confirmation_token(
            operation, agent_code, source_key, event_key)

    def resolve(
        self,
        agent_code: str,
        *,
        operation: str = "locate",
        requested_source: str | None = None,
        confirmation_token: str | None = None,
        event_key: str = "",
        raw_input: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
        audit: bool | None = None,
    ) -> SourceAttribution:
        """解析代理归属。

        优先级：调用方显式指定的源 > 代理登记行显式关联 > 历史别名 >
        ``AGENT-`` 前缀兼容推导。任何一层出现多个不同源都返回 ``ambiguous``，
        不会替调用方选择，也不会触碰候选源的已有位点。
        """
        agent_code = str(agent_code or "").strip()
        received: dict[str, Any] = dict(raw_input or {})
        received.setdefault("agent_code", agent_code)
        if requested_source is not None:
            received.setdefault("requested_source", requested_source)
        if context:
            received.setdefault("context", context)

        explicit = self._explicit_bindings(agent_code)
        alias_rows = self._alias_rows(agent_code)
        alias_sources = _distinct([self._alias_source(row) for row in alias_rows])
        requested = str(requested_source or "").strip()

        source_key: str | None = None
        candidates: tuple[str, ...] = ()
        status = ATTRIBUTION_UNRESOLVED
        reason = "no-attribution"

        if requested:
            source_key, status, reason = requested, ATTRIBUTION_CONFIRMED, "requested-source"
            candidates = (requested,)
        elif len(explicit) == 1:
            source_key, status, reason = explicit[0], ATTRIBUTION_CONFIRMED, "agent-registration"
            candidates = explicit
        elif len(explicit) > 1:
            status, reason, candidates = (
                ATTRIBUTION_AMBIGUOUS,
                "agent-registration-conflict",
                explicit,
            )
        elif len(alias_sources) == 1:
            source_key, status, reason = alias_sources[0], ATTRIBUTION_CONFIRMED, "alias-registry"
            candidates = alias_sources
        elif len(alias_sources) > 1:
            status, reason, candidates = (
                ATTRIBUTION_AMBIGUOUS,
                "alias-ownership-conflict",
                alias_sources,
            )
        elif AGENT_PREFIX in agent_code:
            # 保持历史兼容：旧实现使用 replace("AGENT-", "")，这里不改变已确认源的解析结果。
            derived = agent_code.replace(AGENT_PREFIX, "")
            if derived:
                source_key, status, reason = derived, ATTRIBUTION_DERIVED, "legacy-prefix"
                candidates = (derived,)

        if status != ATTRIBUTION_AMBIGUOUS and source_key is None:
            status = ATTRIBUTION_UNRESOLVED
            reason = reason or "no-attribution"

        if confirmation_token:
            token = confirmation_token
        elif event_key:
            token = self.confirmation_token(operation, agent_code, source_key, event_key)
        else:
            token = _new_token(operation, agent_code, source_key)
        attribution = SourceAttribution(
            agent_code=agent_code,
            operation=operation,
            status=status,
            source_key=source_key,
            candidates=candidates,
            reason=reason,
            raw_input=received,
            input_digest=_digest(received),
            confirmation_token=token,
            alias_sources=tuple(dict(row) for row in alias_rows),
        )

        should_audit = audit is True or (
            audit is None and status in (ATTRIBUTION_AMBIGUOUS, ATTRIBUTION_UNRESOLVED)
        )
        if should_audit:
            self._append_audit(attribution)
        return attribution

    # -- 追查 --

    def _append_audit(self, attribution: SourceAttribution) -> None:
        path = checkpoint_dir() / AUDIT_FILE
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(
                attribution.trace_record(), ensure_ascii=False, default=str,
            ) + "\n")

    def trace(
        self,
        *,
        agent_code: str | None = None,
        confirmation_token: str | None = None,
        ambiguous_only: bool = False,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """回查解析记录；歧义记录里的 ``raw_input`` 就是收到时的原输入。"""
        records = [
            record for record in _read_jsonl(checkpoint_dir() / AUDIT_FILE)
            if (agent_code is None or record.get("agent_code") == agent_code)
            and (confirmation_token is None or record.get("confirmation_token") == confirmation_token)
            and (not ambiguous_only or record.get("ambiguous"))
        ]
        return records[-limit:] if limit else records


class ConfirmationLedger:
    """确认凭据账本：保证同一个凭据只真正更新一次位点。

    记录分三种状态：``claimed``（已受理、尚未完成）、``applied``（已更新）、
    ``failed``（更新失败，允许同一凭据恢复后重试）。失败重试会复用已受理记录，
    因此恢复成功后只留下一次位置更新。
    """

    def __init__(self, filename: str = LEDGER_FILE) -> None:
        self.filename = filename
        self._records: dict[str, dict[str, Any]] | None = None
        self._lock = threading.Lock()

    def _path(self) -> Path:
        return checkpoint_dir() / self.filename

    def _load(self) -> dict[str, dict[str, Any]]:
        if self._records is None:
            path = self._path()
            if path.exists():
                try:
                    with open(path, "r", encoding="utf-8") as handle:
                        self._records = json.load(handle)
                except (OSError, json.JSONDecodeError):
                    self._records = {}
            else:
                self._records = {}
        return self._records

    def _save(self) -> None:
        path = self._path()
        temporary = path.with_suffix(path.suffix + ".tmp")
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(self._load(), handle, ensure_ascii=False, indent=2, default=str)
        os.replace(temporary, path)

    def claim(
        self,
        token: str,
        *,
        operation: str,
        agent_code: str,
        source_key: str,
    ) -> tuple[bool, dict[str, Any]]:
        """尝试占用凭据。返回 ``(是否可执行, 账本记录)``。"""
        with self._lock:
            records = self._load()
            record = records.get(token)
            if record is not None:
                bound = (
                    record.get("operation"),
                    record.get("agent_code"),
                    record.get("source_key"),
                )
                if bound != (operation, agent_code, source_key):
                    return False, {
                        "error": "确认凭据已用于其他代理/源/操作，拒绝重复更新",
                        "status": "conflict",
                        "confirmation_token": token,
                        "previous": record,
                    }
                if record.get("status") == "applied":
                    return False, record
                record["status"] = "claimed"
                record["attempts"] = int(record.get("attempts", 0)) + 1
                record["claimed_at"] = time.time()
                record.pop("error", None)
                self._save()
                return True, record

            record = {
                "confirmation_token": token,
                "operation": operation,
                "agent_code": agent_code,
                "source_key": source_key,
                "status": "claimed",
                "attempts": 1,
                "claimed_at": time.time(),
                "result": None,
            }
            records[token] = record
            self._save()
            return True, record

    def complete(self, token: str, result: dict[str, Any]) -> None:
        with self._lock:
            record = self._load().get(token)
            if record is None:
                return
            record["status"] = "applied"
            record["applied_at"] = time.time()
            record["result"] = result
            self._save()

    def release(self, token: str, error: str) -> None:
        with self._lock:
            record = self._load().get(token)
            if record is None:
                return
            record["status"] = "failed"
            record["error"] = error
            record["failed_at"] = time.time()
            self._save()

    def reset(self) -> None:
        with self._lock:
            self._records = {}
            path = self._path()
            if path.exists():
                path.unlink()


agent_source_resolver = AgentSourceResolver()
confirmation_ledger = ConfirmationLedger()


def resolve_agent_source(agent_code: str, **kwargs: Any) -> SourceAttribution:
    """模块级便捷入口，供管道和其他服务共用同一份解析契约。"""
    return agent_source_resolver.resolve(agent_code, **kwargs)


def get_resolution_trace(**kwargs: Any) -> list[dict[str, Any]]:
    return agent_source_resolver.trace(**kwargs)

"""代理采集位置解析的登记与只读回查接口。

歧义归属不会被静默归并，调用方可在这里按代理编号或确认凭据取回
当时收到的原输入与候选源，用于事后追查。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services.agent_source_resolution import (
    agent_source_resolver,
    get_resolution_trace,
)

router = APIRouter(prefix="/api/agent-source-resolution", tags=["代理采集位置"])


class AliasPayload(BaseModel):
    agent_code: str
    source_key: str
    status: str = "已确认"
    note: str | None = None


@router.get("/trace")
def resolution_trace(
    agent_code: str | None = Query(default=None),
    confirmation_token: str | None = Query(default=None),
    ambiguous_only: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    """返回代理归属解析记录（含歧义时收到的原输入）。"""
    records = get_resolution_trace(
        agent_code=agent_code,
        confirmation_token=confirmation_token,
        ambiguous_only=ambiguous_only,
        limit=limit,
    )
    return {"items": records, "total": len(records)}


@router.post("/aliases")
def register_alias(payload: AliasPayload) -> dict[str, Any]:
    """登记一条历史代理名归属；同名多源会保持为歧义而不是合并。"""
    try:
        entry = agent_source_resolver.register_alias(
            payload.agent_code,
            payload.source_key,
            status=payload.status,
            note=payload.note,
        )
    except ValueError as exc:
        return {"ok": False, "message": str(exc)}

    attribution = agent_source_resolver.resolve(payload.agent_code, operation="alias-register")
    return {
        "ok": True,
        "message": "代理归属已登记",
        "entry": entry,
        "resolution": attribution.as_dict(),
    }


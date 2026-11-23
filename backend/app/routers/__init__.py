"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import agent_resolution, resource

# 固定路径（代理归属回查）需先于通用 /api/{module}/{entry_id} 注册。
ROUTERS = [agent_resolution, resource]

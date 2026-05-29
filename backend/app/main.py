"""
API principal de Global-Relay Sync.
REST endpoints + WebSocket para monitoreo en tiempo real.
"""
import asyncio
import json
import logging
import os

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.cdc_consumer import run_cdc_consumer, REDIS_STATS_KEY, REDIS_CONFLICTS_KEY, REDIS_CHANNEL

logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Global-Relay Sync API",
    description="CDC-based data replication with conflict detection.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

connected_ws: list[WebSocket] = []


@app.on_event("startup")
async def startup():
    app.state.redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    app.state.consumer_task = asyncio.create_task(run_cdc_consumer())
    app.state.broadcast_task = asyncio.create_task(broadcast_to_ws())
    logger.info("✓ Global-Relay Sync iniciado")


@app.on_event("shutdown")
async def shutdown():
    app.state.consumer_task.cancel()
    app.state.broadcast_task.cancel()
    await app.state.redis.aclose()


@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0", "service": "global-relay-sync"}


@app.get("/api/stats")
async def get_stats():
    raw = await app.state.redis.get(REDIS_STATS_KEY)
    if raw:
        return json.loads(raw)
    return {
        "total_events_processed": 0,
        "inserts": 0,
        "updates": 0,
        "deletes": 0,
        "conflicts_detected": 0,
        "avg_replication_latency_ms": 0.0,
        "status": "waiting"
    }


@app.get("/api/conflicts")
async def get_conflicts(limit: int = 50):
    raw = await app.state.redis.lrange(REDIS_CONFLICTS_KEY, 0, limit - 1)
    return [json.loads(r) for r in raw]


@app.get("/api/orders/source")
async def get_source_orders():
    """Retorna los últimos 20 orders de la DB origen."""
    import psycopg2
    import psycopg2.extras
    from app.cdc_consumer import get_db_connection
    conn = get_db_connection(settings.SOURCE_DB_URL)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM orders ORDER BY updated_at DESC LIMIT 20")
        rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/orders/target")
async def get_target_orders():
    """Retorna los últimos 20 orders de la DB destino (réplica)."""
    import psycopg2
    import psycopg2.extras
    from app.cdc_consumer import get_db_connection
    conn = get_db_connection(settings.TARGET_DB_URL)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM orders ORDER BY updated_at DESC LIMIT 20")
        rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_ws.append(websocket)
    logger.info(f"[WS] Cliente conectado. Total: {len(connected_ws)}")
    try:
        stats = await get_stats()
        conflicts = await get_conflicts(limit=10)
        await websocket.send_json({
            "type": "snapshot",
            "data": {"stats": stats, "conflicts": conflicts}
        })
        while True:
            await asyncio.sleep(1)
            await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        connected_ws.remove(websocket)
    except Exception as e:
        logger.error(f"[WS] Error: {e}")
        if websocket in connected_ws:
            connected_ws.remove(websocket)


async def broadcast_to_ws():
    pubsub_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    pubsub = pubsub_client.pubsub()
    await pubsub.subscribe(REDIS_CHANNEL)
    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        if not connected_ws:
            continue
        try:
            payload = json.loads(message["data"])
            dead = []
            for ws in connected_ws:
                try:
                    await ws.send_json(payload)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                connected_ws.remove(ws)
        except Exception as e:
            logger.error(f"[BROADCAST] Error: {e}")
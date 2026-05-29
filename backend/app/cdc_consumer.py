"""
CDC Consumer - captura eventos de cambios desde Kafka y los replica
en la base de datos destino con detección de conflictos.
"""
import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException
import redis.asyncio as aioredis

from app.config import settings
from app.models import ConflictEvent, SyncStats

logger = logging.getLogger(__name__)

# Claves Redis
REDIS_STATS_KEY = "sync:stats"
REDIS_CONFLICTS_KEY = "sync:conflicts"
REDIS_EVENTS_KEY = "sync:events"
REDIS_CHANNEL = "sync:live"

stats = SyncStats()


def get_db_connection(url: str):
    """Crea conexión a PostgreSQL desde URL."""
    url = url.replace("postgresql://", "")
    credentials, rest = url.split("@")
    user, password = credentials.split(":")
    host_port, dbname = rest.split("/")
    host, port = host_port.split(":")
    return psycopg2.connect(
        host=host, port=port, dbname=dbname,
        user=user, password=password
    )


def get_target_version(conn, table: str, record_id: int) -> Optional[int]:
    """Obtiene la versión actual del registro en la DB destino."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT version FROM {table} WHERE id = %s", (record_id,))
        row = cur.fetchone()
        return row[0] if row else None


def apply_insert(conn, table: str, data: dict):
    """Aplica un INSERT en la DB destino."""
    columns = [k for k in data.keys() if k != "id"]
    values = [data[k] for k in columns]
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING",
            values
        )
    conn.commit()


def apply_update(conn, table: str, data: dict, record_id: int):
    """Aplica un UPDATE en la DB destino."""
    columns = [k for k in data.keys() if k not in ("id", "created_at")]
    values = [data[k] for k in columns]
    set_clause = ", ".join([f"{col} = %s" for col in columns])
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {table} SET {set_clause} WHERE id = %s",
            values + [record_id]
        )
    conn.commit()


def apply_delete(conn, table: str, record_id: int):
    """Aplica un DELETE en la DB destino."""
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table} WHERE id = %s", (record_id,))
    conn.commit()


def detect_conflict(before: dict, target_version: int) -> bool:
    """
    Detecta conflicto: si la versión en destino es mayor que la versión
    antes del cambio, alguien modificó el registro mientras tanto.
    """
    if before is None or target_version is None:
        return False
    source_version = before.get("version", 1)
    return target_version > source_version


def _build_alert_message(service: str, metric: str, value: float, severity: str) -> str:
    """Construye mensaje de alerta para logging."""
    return f"[{severity.upper()}] {service}: {metric}={value:.2f}"


async def publish_to_observeiq(producer, event_type: str, message: str, level: str = "INFO"):
    """Envía logs a ObserveIQ via Kafka para monitoreo integrado."""
    try:
        log_event = {
            "id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": "global-relay-sync",
            "level": level,
            "message": message,
            "metadata": {"event_type": event_type}
        }
        producer.produce(
            settings.OBSERVEIQ_KAFKA_TOPIC,
            key="global-relay-sync".encode(),
            value=json.dumps(log_event).encode()
        )
        producer.poll(0)
    except Exception as e:
        logger.error(f"Error publicando a ObserveIQ: {e}")


async def process_cdc_event(
    event: dict,
    target_conn,
    redis_client: aioredis.Redis,
    observeiq_producer
):
    """Procesa un evento CDC y lo replica en la DB destino."""
    global stats

    operation = event.get("op", "").upper()
    table = "orders"
    before = event.get("before")
    after = event.get("after")
    record_id = (after or before or {}).get("id")

    if not record_id:
        return

    start_time = datetime.now(timezone.utc)

    # ── Detección de conflictos ────────────────────────────────────────
    if operation in ("U", "D") and before:
        target_version = get_target_version(target_conn, table, record_id)
        if detect_conflict(before, target_version):
            conflict = ConflictEvent(
                id=str(uuid.uuid4()),
                timestamp=datetime.now(timezone.utc),
                table=table,
                record_id=record_id,
                source_version=before.get("version", 1),
                target_version=target_version,
                source_data=before,
                target_data=after or {},
                resolved=False,
                resolution="last_write_wins"
            )
            stats.conflicts_detected += 1
            await redis_client.lpush(REDIS_CONFLICTS_KEY, conflict.model_dump_json())
            await redis_client.ltrim(REDIS_CONFLICTS_KEY, 0, 99)
            await publish_to_observeiq(
                observeiq_producer,
                "CONFLICT",
                f"Conflicto detectado en {table} id={record_id} — resolviendo con last-write-wins",
                "WARNING"
            )

    # ── Aplicar cambio ─────────────────────────────────────────────────
    try:
        if operation == "C" and after:
            apply_insert(target_conn, table, after)
            stats.inserts += 1
        elif operation == "U" and after:
            apply_update(target_conn, table, after, record_id)
            stats.updates += 1
        elif operation == "D" and before:
            apply_delete(target_conn, table, record_id)
            stats.deletes += 1

        stats.total_events_processed += 1
        stats.last_sync_at = datetime.now(timezone.utc)

        latency = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        stats.avg_replication_latency_ms = (
            (stats.avg_replication_latency_ms * (stats.total_events_processed - 1) + latency)
            / stats.total_events_processed
        )

        live_event = {
            "type": "cdc_event",
            "operation": operation,
            "table": table,
            "record_id": record_id,
            "latency_ms": round(latency, 2),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        await redis_client.publish(REDIS_CHANNEL, json.dumps(live_event))
        await redis_client.setex(REDIS_STATS_KEY, 30, stats.model_dump_json())

        await publish_to_observeiq(
            observeiq_producer,
            f"CDC_{operation}",
            f"Replicado {operation} en {table} id={record_id} ({latency:.1f}ms)",
            "INFO"
        )

    except Exception as e:
        logger.error(f"Error aplicando {operation} en {table}: {e}")
        await publish_to_observeiq(
            observeiq_producer, "ERROR",
            f"Error replicando {operation} en {table} id={record_id}: {e}",
            "ERROR"
        )


async def run_cdc_consumer():
    """Loop principal del consumer CDC."""
    from confluent_kafka import Producer

    redis_client = aioredis.from_url(settings.REDIS_URL)
    target_conn = get_db_connection(settings.TARGET_DB_URL)
    observeiq_producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

    conf = {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "global-relay-sync-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    consumer = Consumer(conf)
    consumer.subscribe([settings.KAFKA_TOPIC_ORDERS])

    logger.info("[CDC] Consumer iniciado — escuchando topic: %s", settings.KAFKA_TOPIC_ORDERS)

    try:
        while True:
            msg = consumer.poll(timeout=0.1)
            if msg is None:
                await asyncio.sleep(0.01)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                await process_cdc_event(payload, target_conn, redis_client, observeiq_producer)
            except Exception as e:
                logger.error("[CDC] Error procesando mensaje: %s", e)
    except asyncio.CancelledError:
        logger.info("[CDC] Consumer detenido.")
    finally:
        consumer.close()
        target_conn.close()
        await redis_client.aclose()
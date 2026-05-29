"""
CDC liviano via PostgreSQL WAL usando test_decoding (built-in).
Captura cambios de la DB origen y los publica en Kafka.
"""
import asyncio
import json
import logging
import re
import uuid
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from confluent_kafka import Producer

from app.config import settings

logger = logging.getLogger(__name__)

SLOT_NAME = "global_relay_slot"


def get_conn():
    url = settings.SOURCE_DB_URL.replace("postgresql://", "")
    credentials, rest = url.split("@")
    user, password = credentials.split(":")
    host_port, dbname = rest.split("/")
    host, port = host_port.split(":")
    return psycopg2.connect(
        host=host, port=port, dbname=dbname,
        user=user, password=password
    )


def ensure_replication_slot(conn):
    """Crea el slot de replicación si no existe."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s",
            (SLOT_NAME,)
        )
        if not cur.fetchone():
            cur.execute(
                "SELECT pg_create_logical_replication_slot(%s, 'test_decoding')",
                (SLOT_NAME,)
            )
            conn.commit()
            logger.info(f"[WAL-CDC] Slot creado: {SLOT_NAME}")
        else:
            logger.info(f"[WAL-CDC] Slot existente: {SLOT_NAME}")


def parse_test_decoding(data: str) -> dict | None:
    """
    Parsea el output de test_decoding a formato CDCEvent.
    Formato típico:
      table public.orders: INSERT: id[integer]:1 customer_id[varchar]:'cust_abc' ...
      table public.orders: UPDATE: id[integer]:1 ... (new values)
      table public.orders: DELETE: id[integer]:1
    """
    if "orders" not in data:
        return None

    op = None
    if ": INSERT:" in data:
        op = "C"
    elif ": UPDATE:" in data:
        op = "U"
    elif ": DELETE:" in data:
        op = "D"
    else:
        return None

    def extract_fields(segment: str) -> dict:
        """Extrae campos clave:valor del segmento de test_decoding."""
        fields = {}
        # Patrón: nombre[tipo]:valor
        pattern = r"(\w+)\[[\w\s]+\]:'?([^'\s]+)'?"
        matches = re.findall(pattern, segment)
        for key, value in matches:
            # Intentar convertir a número si es posible
            try:
                if "." in value:
                    fields[key] = float(value)
                else:
                    fields[key] = int(value)
            except ValueError:
                fields[key] = value
        return fields

    after = None
    before = None

    if op == "C":
        segment = data.split(": INSERT:")[1]
        after = extract_fields(segment)
    elif op == "U":
        segment = data.split(": UPDATE:")[1]
        after = extract_fields(segment)
    elif op == "D":
        segment = data.split(": DELETE:")[1]
        before = extract_fields(segment)

    record_id = (after or before or {}).get("id")
    if not record_id:
        return None

    return {
        "op": op,
        "table": "orders",
        "before": before,
        "after": after,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source_db": "globalrelay",
        "transaction_id": str(uuid.uuid4()),
    }


async def run_wal_cdc():
    """Loop principal del WAL CDC."""
    producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

    conn = get_conn()
    ensure_replication_slot(conn)

    logger.info("[WAL-CDC] Iniciando captura de cambios via WAL (test_decoding)")

    tick = 0
    while True:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT lsn, xid, data FROM pg_logical_slot_get_changes(%s, NULL, NULL)",
                    (SLOT_NAME,)
                )
                rows = cur.fetchall()

            for lsn, xid, data in rows:
                try:
                    event = parse_test_decoding(data)
                    if event:
                        producer.produce(
                            settings.KAFKA_TOPIC_ORDERS,
                            key="orders".encode(),
                            value=json.dumps(event).encode()
                        )
                        logger.info(f"[WAL-CDC] {event['op']} id={( event.get('after') or event.get('before') or {}).get('id')} → Kafka")
                except Exception as e:
                    logger.error(f"[WAL-CDC] Error parseando: {e} — data: {data[:100]}")

            if rows:
                producer.flush()

            conn.commit()

        except Exception as e:
            logger.error(f"[WAL-CDC] Error: {e}")
            try:
                conn = get_conn()
            except Exception:
                pass

        tick += 1
        if tick % 20 == 0:
            logger.info(f"[WAL-CDC] Tick {tick} — activo")

        await asyncio.sleep(0.5)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_wal_cdc())
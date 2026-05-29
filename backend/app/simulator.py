"""
Simulador de cambios en la DB origen.
Genera operaciones INSERT, UPDATE y DELETE realistas
directamente en PostgreSQL para simular una app en producción.
"""
import asyncio
import logging
import random
import uuid
from datetime import datetime

import psycopg2

from app.config import settings

logger = logging.getLogger(__name__)

PRODUCTS = [
    "MacBook Pro 16", "iPhone 15 Pro", "AirPods Pro",
    "Samsung Galaxy S24", "Sony WH-1000XM5", "iPad Air",
    "Dell XPS 15", "LG OLED 55", "Nintendo Switch",
    "PlayStation 5"
]

STATUSES = ["pending", "processing", "completed", "cancelled"]


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


def insert_order(conn) -> int:
    """Inserta un nuevo pedido y retorna su ID."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO orders (customer_id, product, quantity, price, status)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (
            f"cust_{uuid.uuid4().hex[:8]}",
            random.choice(PRODUCTS),
            random.randint(1, 5),
            round(random.uniform(50, 3000), 2),
            "pending"
        ))
        order_id = cur.fetchone()[0]
    conn.commit()
    return order_id


def update_order(conn, order_id: int):
    """Actualiza el status y versión de un pedido."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE orders
            SET status = %s,
                updated_at = NOW(),
                version = version + 1
            WHERE id = %s
        """, (random.choice(STATUSES), order_id))
    conn.commit()


def delete_order(conn, order_id: int):
    """Elimina un pedido."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM orders WHERE id = %s", (order_id,))
    conn.commit()


def get_random_order_id(conn) -> int | None:
    """Obtiene un ID aleatorio de la tabla."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM orders ORDER BY RANDOM() LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else None


async def run_simulator():
    """Loop principal del simulador."""
    conn = get_conn()
    inserted_ids = []
    tick = 0

    logger.info("[SIMULATOR] Iniciando simulación de cambios en DB origen")

    while True:
        operation = random.choices(
            ["INSERT", "UPDATE", "DELETE"],
            weights=[50, 35, 15],
            k=1
        )[0]

        try:
            if operation == "INSERT":
                order_id = insert_order(conn)
                inserted_ids.append(order_id)
                logger.info(f"[SIM] INSERT order id={order_id}")

            elif operation == "UPDATE" and inserted_ids:
                order_id = random.choice(inserted_ids)
                update_order(conn, order_id)
                logger.info(f"[SIM] UPDATE order id={order_id}")

            elif operation == "DELETE" and len(inserted_ids) > 5:
                order_id = inserted_ids.pop(0)
                delete_order(conn, order_id)
                logger.info(f"[SIM] DELETE order id={order_id}")

        except Exception as e:
            logger.error(f"[SIM] Error en {operation}: {e}")
            try:
                conn = get_conn()
            except Exception:
                pass

        tick += 1
        if tick % 20 == 0:
            logger.info(f"[SIM] Tick {tick} — {len(inserted_ids)} orders activos en DB")

        await asyncio.sleep(random.uniform(0.5, 1.5))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_simulator())
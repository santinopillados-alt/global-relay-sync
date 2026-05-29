"""
Modelos de datos para Global-Relay Sync.
"""
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional
from pydantic import BaseModel
import uuid


class OrderStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class Order(BaseModel):
    id: Optional[int] = None
    customer_id: str
    product: str
    quantity: int
    price: Decimal
    status: OrderStatus = OrderStatus.PENDING
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    version: int = 1

    model_config = {"use_enum_values": True}


class CDCEvent(BaseModel):
    """Evento CDC capturado desde Kafka/Debezium."""
    operation: str        # INSERT, UPDATE, DELETE
    table: str
    before: Optional[dict] = None   # estado anterior (para UPDATE/DELETE)
    after: Optional[dict] = None    # estado nuevo (para INSERT/UPDATE)
    timestamp: datetime
    source_db: str
    transaction_id: Optional[str] = None

    model_config = {"use_enum_values": True}


class ConflictEvent(BaseModel):
    """Conflicto detectado durante la replicación."""
    id: str = str(uuid.uuid4())
    timestamp: datetime = datetime.utcnow()
    table: str
    record_id: int
    source_version: int
    target_version: int
    source_data: dict
    target_data: dict
    resolved: bool = False
    resolution: Optional[str] = None


class SyncStats(BaseModel):
    """Estadísticas de sincronización en tiempo real."""
    total_events_processed: int = 0
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    conflicts_detected: int = 0
    conflicts_resolved: int = 0
    avg_replication_latency_ms: float = 0.0
    last_sync_at: Optional[datetime] = None
    status: str = "running"
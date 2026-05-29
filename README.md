# Global-Relay Sync — Real-Time CDC Data Replication Platform

> An event-driven data synchronization pipeline that captures PostgreSQL changes via Write-Ahead Log (WAL), streams them through Apache Kafka, and replicates them to a target database with automatic conflict detection and sub-100ms latency.

---

## Architecture Overview
┌─────────────────┐     ┌──────────────────────────────────────────┐
│   Source DB      │     │           WAL CDC Pipeline               │
│   PostgreSQL     │────▶│  ┌─────────────┐  ┌──────────────────┐  │
│   (globalrelay)  │     │  │test_decoding│  │  Kafka Producer  │  │
└─────────────────┘     │  └──────┬──────┘  └────────┬─────────┘  │
└─────────┼───────────────────┼────────────┘
│                   │
▼                   ▼
┌──────────────────────────────────┐
│         Apache Kafka             │
│   globalrelay.public.orders      │
└──────────────┬───────────────────┘
│ consume
▼
┌─────────────────┐
│  FastAPI Backend │
│  ┌─────────────┐│
│  │CDC Consumer ││
│  └──────┬──────┘│
│         │        │
│  ┌──────▼──────┐│
│  │  Conflict   ││
│  │  Detector   ││
│  └──────┬──────┘│
│         │        │
│  ┌──────▼──────┐│
│  │    Redis    ││
│  │   Pub/Sub   ││
│  └──────┬──────┘│
└─────────┼────────┘
│ WebSocket
▼
┌─────────────────┐     ┌─────────────────┐
│  React Dashboard │     │   Target DB     │
│  Live CDC events │     │   PostgreSQL    │
│  Conflict panel  │     │   (replica)     │
└─────────────────┘     └─────────────────┘
## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Source DB** | PostgreSQL 17 | Primary database with WAL enabled |
| **CDC Mechanism** | PostgreSQL logical decoding | Captures row-level changes |
| **Message Broker** | Apache Kafka 3.9 | Event streaming backbone |
| **Backend** | FastAPI (Python 3.11) | REST API + WebSocket server |
| **Cache/State** | Redis 7 | Event store + pub/sub channel |
| **Frontend** | React 18 + Vite | Real-time replication dashboard |
| **Target DB** | PostgreSQL 17 | Replica database |

## Key Features

- **WAL-based CDC**: captures every INSERT, UPDATE, DELETE directly from PostgreSQL's write-ahead log — no triggers, no polling overhead
- **Sub-100ms replication latency**: changes propagate from source to target in under 10ms on local setup
- **Automatic conflict detection**: detects version conflicts using optimistic locking — when two versions of the same record diverge, the system flags it and applies last-write-wins resolution
- **Event streaming**: full pipeline from DB change to React dashboard via Kafka → Redis pub/sub → WebSocket
- **ObserveIQ integration**: replication events are forwarded to the ObserveIQ platform for cross-system monitoring

## Running Locally

**Prerequisites:** PostgreSQL 17, Apache Kafka 3.9, Redis, Python 3.11, Node.js 20

```bash
git clone https://github.com/santinopillados-alt/global-relay-sync
cd global-relay-sync
```

**1 — Configure PostgreSQL WAL**
```sql
ALTER SYSTEM SET wal_level = logical;
-- Restart PostgreSQL after this
```

**2 — Start Kafka and Redis**
```bash
# Kafka
kafka-server-start.bat config/kraft/server.properties

# Redis
redis-server
```

**3 — Start backend**
```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --port 8002 --ws websockets
```

**4 — Start CDC pipeline**
```bash
python -m app.wal_cdc      # WAL capture → Kafka
python -m app.simulator    # generates test data
```

**5 — Start frontend**
```bash
cd frontend
npm install
npm run dev -- --port 3001
```

Dashboard → http://localhost:3001

## API Reference
GET  /health              → Service health check
GET  /api/stats           → Replication statistics
GET  /api/conflicts       → Detected conflicts
GET  /api/orders/source   → Latest records in source DB
GET  /api/orders/target   → Latest records in target DB
WS   /ws                  → Real-time CDC event stream
## Conflict Resolution Strategy

When a conflict is detected (target version > source version before the change):

1. The conflict is logged with full before/after snapshots
2. **Last-write-wins** is applied as the default resolution
3. The conflict is stored in Redis and surfaced in the dashboard
4. An alert is forwarded to ObserveIQ for cross-platform visibility

## Design Decisions

**Why WAL over triggers?**
Trigger-based CDC adds write overhead to every transaction. WAL-based CDC reads the replication log asynchronously — zero impact on source DB performance, and changes are captured even if the consumer is temporarily offline (Kafka retains the messages).

**Why Kafka between WAL and consumer?**
Decoupling the capture from the apply step means the consumer can lag behind without losing events. Kafka's offset management also enables replay — if the target DB goes down, we can re-consume from any point in history.

**Why optimistic locking for conflict detection?**
Each record carries a `version` counter. On UPDATE, the consumer compares the expected version against the current target version. If they diverge, a conflict is raised. This is the same pattern used by Hibernate, Django ORM, and most enterprise replication systems.

---

Built by Santino — Portfolio project demonstrating CDC architecture, distributed data pipelines, and real-time replication systems.
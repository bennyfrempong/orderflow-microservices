# OrderFlow — Microservices Order-Processing System

A production-style microservices system demonstrating async event-driven architecture, service isolation, and distributed failure handling via the Saga pattern.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         CLIENT / FRONTEND                            │
│                      frontend/index.html                             │
└────────────────────────────┬─────────────────────────────────────────┘
                             │ POST /orders
                             ▼
┌─────────────────────────────────────────┐
│           ORDER SERVICE  :5001          │
│  • Accepts & validates orders           │
│  • Writes to own SQLite DB              │
│  • Publishes OrderCreated event         │
│  • Listens for InventoryReserved        │
│    → marks order "confirmed"            │
│  • Listens for OutOfStock (SAGA)        │
│    → marks order "cancelled"            │
└────────────────┬────────────────────────┘
                 │ OrderCreated
                 ▼
        ┌────────────────┐
        │   RabbitMQ     │  (topic exchange: orders)
        │  :5672 / 15672 │
        └────────┬───────┘
                 │ order.created
                 ▼
┌─────────────────────────────────────────┐
│        INVENTORY SERVICE  :5002         │
│  • Listens for OrderCreated             │
│  • Checks own SQLite DB for stock       │
│  • If sufficient: decrements stock      │
│    → publishes InventoryReserved        │
│  • If insufficient:                     │
│    → publishes OutOfStock               │
└────────────────┬────────────────────────┘
                 │ inventory.reserved
                 │ inventory.out_of_stock
                 ▼
        ┌────────────────┐
        │   RabbitMQ     │  (topic exchange: inventory)
        └────────┬───────┘
                 │
        ┌────────┴────────────────────────┐
        ▼                                 ▼
┌───────────────────┐       ┌─────────────────────────┐
│ NOTIFICATION :5003│       │  ORDER SERVICE (Saga)   │
│ Logs confirmation │       │  OutOfStock received →  │
│ or rejection email│       │  order.status=cancelled │
└───────────────────┘       └─────────────────────────┘
```

---

## Tech Stack

| Layer | Choice | Reason |
|-------|--------|--------|
| Services | Flask (Python 3.12) | Lightweight, interview-standard |
| Message Broker | RabbitMQ 3 (topic exchanges) | Industry standard, has management UI |
| Database | SQLite (per service) | True data isolation — not a shared-DB monolith |
| Containerization | Docker + docker-compose | One-command local demo |
| Testing | pytest + unittest.mock | Standard Python testing |
| Frontend | Plain HTML/JS | Zero build step, clean demo |

---

## Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) or [Rancher Desktop](https://rancherdesktop.io/)
- That's it — Python is not required on the host machine

### Run the system

```bash
git clone <repo-url>
cd project-1.0

docker compose up --build
```

All 4 services start automatically:
| Service | Port | Purpose |
|---------|------|---------|
| Order Service | 5001 | REST API for placing orders |
| Inventory Service | 5002 | Stock management |
| Notification Service | 5003 | Logs confirmation/rejection |
| RabbitMQ Management | 15672 | Event flow visualizer (guest/guest) |

### Open the frontend

Open `frontend/index.html` directly in your browser — no build step needed.

---

## API Reference

### Order Service — `localhost:5001`

```
POST /orders
Content-Type: application/json

{
  "product_id":     "PROD-001",
  "quantity":       2,
  "customer_email": "user@example.com"
}

→ 201 { "id": 1, "status": "pending", ... }
```

```
GET /orders/{id}    → { "id": 1, "status": "confirmed" | "cancelled" | "pending" }
GET /orders         → [ ...all orders ]
GET /health         → { "status": "ok" }
```

### Inventory Service — `localhost:5002`

```
GET /products         → [ { "id": "PROD-001", "name": "...", "stock_quantity": 50 } ]
GET /products/{id}    → single product
GET /health           → { "status": "ok" }
```

---

## Demo Walkthrough

### Happy Path (stock available)
```bash
# Place an order for PROD-001 (50 in stock)
curl -X POST http://localhost:5001/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id":"PROD-001","quantity":2,"customer_email":"you@example.com"}'

# Wait ~3 seconds, then check status
curl http://localhost:5001/orders/1
# → "status": "confirmed"

# Check inventory decremented
curl http://localhost:5002/products/PROD-001
# → "stock_quantity": 48
```

### Failure Path + Saga (out of stock)
```bash
# Place an order for PROD-005 (0 stock)
curl -X POST http://localhost:5001/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id":"PROD-005","quantity":1,"customer_email":"you@example.com"}'

# Wait ~3 seconds, then check status
curl http://localhost:5001/orders/2
# → "status": "cancelled"   ← Saga compensation applied

# Check notification logs
docker compose logs notification-service
# → ❌ REJECTED — item is currently out of stock
```

---

## Failure Handling — Saga Pattern

### The Problem
In a distributed system you cannot use a single database transaction across services. When an order is written to the Order DB and then inventory reports out-of-stock, you can't simply "rollback" across service boundaries.

### The Solution: Choreography-Based Saga
Instead of a central orchestrator, each service reacts to events and publishes compensating events if something goes wrong.

```
1. Order Service writes order (status: "pending") ← point of no return
2. Publishes OrderCreated
3. Inventory Service receives it — checks stock
4a. If in stock:  decrements DB → publishes InventoryReserved
    Order Service receives InventoryReserved → status = "confirmed"

4b. If out of stock: publishes OutOfStock (compensating event)
    Order Service receives OutOfStock → status = "cancelled"  ← Saga!
    Notification Service receives OutOfStock → logs rejection
```

**The key insight:** you can't undo the order write, so you write again with a corrected state. That's the compensating transaction.

### Dead-Letter Queues
Every queue is backed by a DLQ. Messages that fail processing after `MAX_RETRIES` attempts are routed to `*.dlq` queues and are visible in the RabbitMQ management UI at `localhost:15672`.

---

## Running Tests

```bash
# Order Service
cd order-service
pip install -r requirements.txt
pytest tests/ -v

# Inventory Service
cd inventory-service
pip install -r requirements.txt
pytest tests/ -v

# Notification Service
cd notification-service
pip install -r requirements.txt
pytest tests/ -v
```

### What's tested
| Service | Tests |
|---------|-------|
| Order | Input validation, DB write, event publish, Saga cancellation, resilience (publish failure) |
| Inventory | Seed data, stock check, decrement, OOS path, unknown product, stock unchanged on OOS |
| Notification | Confirmation handler, rejection handler, retry logic, DLQ escalation, health endpoint |

---

## Project Structure

```
project-root/
├── docker-compose.yml
├── frontend/
│   └── index.html              # Demo UI
├── order-service/
│   ├── app.py                  # Flask API
│   ├── models.py               # Order model
│   ├── publisher.py            # OrderCreated publisher
│   ├── consumer.py             # Saga: InventoryReserved / OutOfStock
│   ├── requirements.txt
│   ├── Dockerfile
│   └── tests/test_order.py
├── inventory-service/
│   ├── app.py                  # Flask app + stock endpoints
│   ├── models.py               # Product model + seed data
│   ├── publisher.py            # InventoryReserved / OutOfStock publisher
│   ├── consumer.py             # OrderCreated consumer
│   ├── requirements.txt
│   ├── Dockerfile
│   └── tests/test_inventory.py
└── notification-service/
    ├── app.py                  # Flask health endpoint
    ├── consumer.py             # InventoryReserved / OutOfStock consumer
    ├── requirements.txt
    ├── Dockerfile
    └── tests/test_notification.py
```

---

## Design Decisions

**Why RabbitMQ over direct HTTP calls?**
HTTP coupling means if Inventory is down, Order fails too. With a message broker, Order Service writes to the queue and returns immediately — Inventory processes when it's ready. Services are truly independent.

**Why separate databases per service?**
A shared database creates hidden coupling — schema changes in one service break others. Each service owns its data and exposes it only through events or its own API. This is what makes it a true microservice, not a "monolith split into files."

**Why the Saga pattern over a 2-phase commit?**
2PC requires all services to coordinate in a single transaction — impossible across independent services with their own DBs. The Saga pattern uses compensating events instead, which is resilient, scalable, and industry-standard for distributed systems.

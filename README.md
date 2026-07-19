# Real-Time Geolocation Dispatch Engine

A high-concurrency real-time matching service designed for ride-sharing and delivery platforms (similar to Uber or Swiggy). The system matches passengers and orders with the nearest active drivers using WebSockets for duplex signaling and Redis for low-latency geospatial indexing.

---

## 🚀 Key Features

* **Real-Time Telemetry & Signaling:** Bi-directional WebSockets connection manager tracking online states and coordinates for riders and drivers.
* **Low-Latency Spatial Indexing:** Leverages Redis Geo APIs (`GEOADD`, `GEOSEARCH`) to index driver coordinates and execute sub-millisecond proximity queries within a configurable search radius (default $5\text{km}$).
* **Reliable Persistence Tier:** Integrated SQLAlchemy ORM with PostgreSQL database featuring connection pooling and automatic schema generation. Includes a SQLite fallback for offline development.
* **Asynchronous Match Loop:** Non-blocking background worker processes that handle ride request timeouts, driver offers, and telemetry state cleanups.
* **Telemetry Simulator:** Stress-testing utility (`simulator.py`) that generates high-frequency driver locations to validate database writes and WebSockets concurrency under load.

---

## 🏗️ System Architecture

```mermaid
graph TD
    subgraph Clients
        Rider[Rider App]
        Driver[Driver App]
    end

    subgraph API Gateway & Service Layer
        FastAPI[FastAPI Application]
        WSManager[WebSocket Connection Manager]
        MatchEngine[Async Dispatch Match Loop]
    end

    subgraph Data Stores
        Redis[Redis Cache & Geo Index]
        DB[(PostgreSQL / SQLite)]
    end

    Rider -->|Request Ride HTTP| FastAPI
    Driver -->|Telemetry / Signaling WS| WSManager
    WSManager -->|GEOADD Coordinates| Redis
    MatchEngine -->|GEOSEARCH Active Drivers| Redis
    MatchEngine -->|Push Offer WS| WSManager
    FastAPI -->|Write Ride History| DB
```

---

## 🛠️ Technology Stack

* **Language:** Python 3.12+
* **Framework:** FastAPI, Uvicorn, Pydantic v2
* **Geospatial Cache:** Redis (Geo APIs)
* **Database:** PostgreSQL (Primary), SQLite (Local Fallback), SQLAlchemy ORM
* **Concurrence & Tasks:** Asyncio Event Loops, WebSockets

---

## 📋 API Endpoints

### WebSockets (Real-time Signaling)
* `WS /ws/driver/{driver_id}` – Persistent telemetry feed; receives ride matching offers.
* `WS /ws/rider/{rider_id}` – Real-time status tracker for active ride orders.

### HTTP REST APIs
* `POST /driver/register` – Registers a new driver profile in database.
* `POST /ride/request` – Requests a new ride based on passenger coordinates.
* `GET /rides/active` – Returns a list of all active rides and matching states.

---

## ⚙️ Local Setup & Installation

### 1. Run Data Infrastructure (Docker)
Ensure Docker is installed and running, then spin up PostgreSQL and Redis:
```bash
docker-compose up -d
```

### 2. Configure Environment Variables
Create a local `.env` file in the root directory:
```env
DEBUG=True
DATABASE_URL=postgresql://dispatch_user:dispatch_password@127.0.0.1:5432/dispatch_db
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
GEOSEARCH_RADIUS_KM=5.0
DRIVER_OFFER_TIMEOUT_SEC=10
```

### 3. Setup Virtual Environment
```bash
# Create venv
python -m venv venv

# Activate venv
# Windows:
.\venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 4. Run the API Server
```bash
uvicorn app.main:app --reload --port 8000
```

### 5. Run the Telemetry Simulator
To test real-time matching and simulate active driver telemetry, run the simulator script in a separate terminal:
```bash
python simulator.py
```

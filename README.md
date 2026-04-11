# Edge Collector Service

> Production-ready OPC UA → RabbitMQ bridge for IIoT telemetry, designed to run on edge devices (industrial PCs, Raspberry Pi).

## Architecture

```
┌─────────────────┐   HTTPS    ┌──────────────┐
│  Cloud SaaS API │ ◄──────────│              │
│  (Control Plane)│            │              │   AMQP    ┌───────────┐
└─────────────────┘   GET      │   Edge       │ ─────────►│ RabbitMQ  │
                    /config    │   Collector  │  batched  │           │
┌─────────────────┐            │   Service    │  JSON     └───────────┘
│  OPC UA Server  │ ◄──────────│              │
│  (Factory Floor)│ subscription│              │
└─────────────────┘            └──────────────┘
                                    :8080/healthz
```

**Control Plane** — Fetches OPC UA configuration (server URL, sensors, auth) from the Cloud REST API on startup and then refreshes it periodically.

**Data Plane** — Subscribes to OPC UA data-change notifications, persists payloads in a local SQLite buffer, and publishes batched telemetry to RabbitMQ.

## Quick Start

### Prerequisites

- Python 3.12
- uv 0.11.2
- Docker & Docker Compose (for local RabbitMQ)

### 1. Clone & install

```bash
uv sync --group dev
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your Cloud API URL, API key ID + secret, and AMQP details
```

### 3. Run with Docker Compose (recommended)

```bash
docker compose up -d rabbitmq   # start RabbitMQ first
docker compose up collector     # start the collector
```

### 4. Run locally (development)

```bash
# Ensure RabbitMQ is running (e.g. via docker compose up -d rabbitmq)
uv run python -m app.main
```

### 5. Run tests

```bash
uv run pytest -v
```

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `CLOUD_API_URL` | ✅ | — | Cloud SaaS API base URL |
| `X_API_KEY_ID` | ✅ | — | API key ID (32-char hex) — copied from `POST /opc-servers/{id}/api-keys` response |
| `X_API_KEY_SECRET` | ✅ | — | API key secret (base64url) — copied from `POST /opc-servers/{id}/api-keys` response |
| `AMQP_URL` | ✅ | — | RabbitMQ connection string |
| `AMQP_EXCHANGE` | ✅ | `iiot_telemetry` | AMQP exchange name |
| `OPCUA_CERT_PATH` | ❌ | `None` | Client certificate for encrypted OPC UA policies |
| `OPCUA_KEY_PATH` | ❌ | `None` | Client private key for encrypted OPC UA policies |
| `HEALTH_HOST` | ❌ | `0.0.0.0` | Health endpoint bind address |
| `HEALTH_PORT` | ❌ | `8080` | Health endpoint port |
| `EDGE_BUFFER_DB_PATH` | ❌ | `edge_buffer.db` | Local SQLite file for durable store-and-forward |
| `QUEUE_MAX_SIZE` | ❌ | `10000` | Deprecated legacy setting from in-memory queue implementation |
| `BATCH_SIZE` | ❌ | `100` | Max messages per AMQP batch |
| `BATCH_TIMEOUT_S` | ❌ | `1.0` | Max seconds to wait before publishing a partial batch |
| `AMQP_HEARTBEAT_S` | ❌ | `15` | Native AMQP heartbeat interval in seconds for RabbitMQ connections (`0` disables client-side request) |
| `CONFIG_REFRESH_INTERVAL_S` | ❌ | `300.0` | Base interval (seconds) between Cloud config refresh requests |
| `CONFIG_REFRESH_JITTER_S` | ❌ | `10.0` | Random offset (0..N seconds) added to each config refresh interval |
| `BACKOFF_BASE_S` | ❌ | `1.0` | Initial delay used for AMQP reconnect retries |
| `BACKOFF_MAX_S` | ❌ | `60.0` | Maximum delay cap used for AMQP reconnect retries |
| `BACKOFF_MAX_RETRIES` | ❌ | `5` | Maximum AMQP reconnect attempts per retry cycle |

## Health Endpoint

```
GET :8080/healthz

200  {"status": "healthy",  "opcua_connected": true,  "amqp_connected": true}
503  {"status": "degraded", "opcua_connected": true,  "amqp_connected": false}
```

## Project Structure

```
app/
├── main.py                     # Orchestrator, signal handling, lifecycle
├── settings.py                 # pydantic-settings (env vars)
├── logging.py                  # structlog JSON configuration
├── health.py                   # /healthz aiohttp server
├── models/
│   ├── config.py               # CollectorConfig, SensorConfig
│   └── telemetry.py            # TelemetryPayload, TelemetryValue
├── control_plane/
│   └── api_client.py           # Cloud API client with retries
├── data_plane/
│   ├── opcua_subscriber.py     # asyncua subscription handler
│   └── amqp_publisher.py       # aio_pika batch publisher
└── utils/
    └── backoff.py              # Exponential backoff helper
```

## Resilience Features

- **Exponential backoff** on Cloud API, RabbitMQ connection failures
- **Dynamic config reload** — periodically refreshes Cloud config and reconfigures OPC UA subscriptions when runtime fields change
- **Durable store-and-forward** — telemetry is persisted in SQLite and survives process/device restarts
- **At-least-once handoff** — records are deleted only after successful AMQP publish
- **Graceful shutdown** — `SIGINT`/`SIGTERM` triggers clean OPC UA disconnect and AMQP shutdown

## Publisher Worker Logic (Simplified)

```python
while True:
    rows = await buffer.get_batch(batch_size=100)
    if not rows:
        await asyncio.sleep(1.0)
        continue

    ids = [row["id"] for row in rows]
    payloads = [row["payload"] for row in rows]

    try:
        await exchange.publish(make_message(payloads), routing_key="telemetry")
        await buffer.commit(ids)  # delete only after successful publish
    except aio_pika.exceptions.CONNECTION_EXCEPTIONS:
        await asyncio.sleep(1.0)  # retry later, do not commit
```

## License

Proprietary — Internal use only.

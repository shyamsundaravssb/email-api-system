# Generation API (Part B)

## 1. Implementation and Usage

The Generation API is Part B of the Email API System. It receives accepted email records from the Ingestion API (Part A), retrieves recipient information and templates from PostgreSQL, generates personalized email content, and publishes messages to RabbitMQ.

The Sender Worker (Part C) consumes these messages and handles actual SMTP/KumoMTA delivery.

### Prerequisites

- Docker and Docker Compose
- PostgreSQL
- RabbitMQ
- Configured environment variables in the repository's `.env` file
- Existing batch, accepted email records, and templates in PostgreSQL

### Configuration

The following environment variables are used for integrated operation:

```env
GENERATION_MODE=integrated
INTERNAL_TOKEN=your_internal_token

POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=your_database_user
POSTGRES_PASSWORD=your_database_password
POSTGRES_DB=your_database_name

RABBITMQ_URL=your_rabbitmq_url

TRACK_BASE_URL=http://localhost:8002
```

Use the actual credentials and tracking service URL configured for your environment. Never commit `.env` or expose credentials.

### Start the Generation API

From the repository root:

```bash
docker compose up -d postgres rabbitmq generation-api
```

To build or rebuild the service:

```bash
docker compose build generation-api
docker compose up -d generation-api
```

### Check the API

Base URL:

```text
http://127.0.0.1:8001
```

Health check:

```bash
curl http://127.0.0.1:8001/health
```

Readiness check:

```bash
curl http://127.0.0.1:8001/health/ready
```

Interactive API documentation:

```text
http://127.0.0.1:8001/docs
```

### Generate Emails

**Endpoint**

```http
POST /internal/generate
```

**Authentication header**

```text
X-Internal-Token: YOUR_INTERNAL_TOKEN
```

**Example request**

```json
{
  "batch_id": "11111111-1111-4111-8111-111111111111",
  "from_email": "sender@example.com",
  "from_name": "Example Team",
  "items": [
    {
      "email_id": "22222222-2222-4222-8222-222222222222",
      "tracking_token": "33333333-3333-4333-8333-333333333333",
      "template_id": "otp_email",
      "to_email": "recipient@example.com",
      "to_name": "Asha",
      "variables": {
        "name": "Asha",
        "otp": "482913"
      }
    }
  ]
}
```

The UUIDs must correspond to actual records in PostgreSQL. The email must belong to the specified batch and have `accepted` status.

**Example curl request**

```bash
curl -X POST http://127.0.0.1:8001/internal/generate \
  -H "Content-Type: application/json" \
  -H "X-Internal-Token: YOUR_INTERNAL_TOKEN" \
  -d '{
    "batch_id": "11111111-1111-4111-8111-111111111111",
    "from_email": "sender@example.com",
    "from_name": "Example Team",
    "items": [{
      "email_id": "22222222-2222-4222-8222-222222222222",
      "tracking_token": "33333333-3333-4333-8333-333333333333",
      "template_id": "otp_email",
      "to_email": "recipient@example.com",
      "to_name": "Asha",
      "variables": {
        "name": "Asha",
        "otp": "482913"
      }
    }]
  }'
```

Replace the example values with actual database records and your configured internal token.

**Successful response — HTTP 202 Accepted**

```json
{
  "batch_id": "11111111-1111-4111-8111-111111111111",
  "received": 1
}
```

HTTP 202 means the request was accepted and processing was scheduled. It does not guarantee that the message has already been published or delivered.

### Preview Generated Emails

The preview endpoint renders sample email content without publishing messages to RabbitMQ.

```http
POST /internal/generate/preview
```

Refer to `/docs` for the preview request schema.

### Email Tracking

The Generation API supports tracking during email generation:

- **Click tracking:** Rewrites supported HTML links with unique tracking URLs.
- **Open tracking:** Adds an open-tracking pixel to the generated HTML.
- **Database mapping:** Stores the email ID, unique link token, and original destination URL in the `tracking_links` table.

The tracking service must be available at the configured `TRACK_BASE_URL` to receive tracking requests.

### Useful Commands

View logs:

```bash
docker compose logs -f generation-api
```

Check service status:

```bash
docker compose ps generation-api
```

Stop the service:

```bash
docker compose stop generation-api
```

---

## 2. Generation API Project Structure

```text
services/
└── generation-api/
    ├── Dockerfile
    ├── requirements.txt
    └── app/
        ├── __init__.py
        ├── main.py
        ├── config.py
        ├── schemas.py
        ├── db.py
        ├── rendering.py
        ├── generation_engine.py
        ├── publisher.py
        ├── processor.py
        └── routes/
            ├── __init__.py
            ├── generate.py
            └── health.py
```

### File Responsibilities

| File | Responsibility |
|---|---|
| `Dockerfile` | Builds the Generation API Docker image and starts the FastAPI application. |
| `requirements.txt` | Defines the Python dependencies required by the service. |
| `main.py` | Creates the FastAPI application and manages startup and shutdown. |
| `config.py` | Loads database, RabbitMQ, authentication, tracking, and processing configuration. |
| `schemas.py` | Defines and validates API request and response models using Pydantic. |
| `db.py` | Manages PostgreSQL connections, retrieves email and template records, stores tracking links, and updates email statuses. |
| `rendering.py` | Renders personalized subject, HTML, and text content using Jinja2. Handles click-link rewriting and open-pixel insertion. |
| `generation_engine.py` | Provides standalone in-memory generation and preview functionality. |
| `publisher.py` | Publishes generated email messages to RabbitMQ with publisher confirmations. |
| `processor.py` | Coordinates email retrieval, rendering, tracking integration, RabbitMQ publishing, and database status updates. |
| `routes/generate.py` | Implements generation and preview endpoints, including internal authentication. |
| `routes/health.py` | Implements health and readiness endpoints. |

---

## 3. Tests Performed

The following manual tests were performed during development.

| Test | Purpose / Observed Result |
|---|---|
| Single-email generation | Tested personalized email rendering and message publication. |
| 5-recipient batch | Tested batch processing and queued status updates. |
| 50-recipient batch | Tested processing with multiple recipients. |
| 1000-recipient batch | Tested large-batch processing and queue-capacity handling. |
| Queue-full test | Verified that rejected RabbitMQ publications do not incorrectly update emails to `queued`. |
| 10-recipient tracking test | Created 10 unique recipients and tracking tokens and verified tracking-link records in PostgreSQL. |
| Tracking-link inspection | Verified stored link tokens and original destination URLs in `tracking_links`. |
| Health and readiness checks | Tested API health and dependency readiness endpoints. |

### Manual Tracking Test

Run the tracking test from the repository root:

```bash
python3 -u tests/manual/test_tracking_10.py
```

This script creates a new batch with 10 randomly generated recipients and submits it to the Generation API.

### Email Processing Status

| Status | Meaning |
|---|---|
| `accepted` | Email is eligible for generation. |
| `queued` | Message publication to RabbitMQ was confirmed. |
| `failed` | Generation or processing failed. |
| `sent` | Downstream delivery status managed by the Sender Worker. |

**Note:** The Generation API handles email content generation and RabbitMQ publication. Actual SMTP/KumoMTA delivery is handled by Part C.
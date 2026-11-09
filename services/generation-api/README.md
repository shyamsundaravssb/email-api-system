## Quick Start – How to Use the Generation API

### 1. What does this API do?

The Generation API is Part B of the Email API System.

It receives email-generation requests from the Ingestion API (Part A), retrieves the corresponding accepted email records and templates from PostgreSQL, generates personalized email content, and publishes messages to RabbitMQ.

It does not send emails directly. The Sender Worker (Part C) handles actual SMTP/KumoMTA delivery.

### 2. Prerequisites

Before using the integrated generation endpoint, make sure:

- Docker and Docker Compose are installed.
- PostgreSQL is running and the required database schema exists.
- RabbitMQ is running and configured.
- The Generation API has valid database and RabbitMQ credentials.
- `GENERATION_MODE=integrated` is configured.
- `INTERNAL_TOKEN` is configured.
- The requested batch and accepted email records already exist in PostgreSQL.
- The requested templates exist in PostgreSQL.

### 3. Start the required services

From the repository root:

```bash
docker compose up -d postgres rabbitmq generation-api
```

### 4. Check the API

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

### 5. Generate emails

**Endpoint:**

```text
POST /internal/generate
```

**Authentication header:**

```text
X-Internal-Token: YOUR_INTERNAL_TOKEN
```

**Example request body:**

```json
{
  "batch_id": "00000000-0000-0000-0000-000000000001",
  "from_email": "sender@example.com",
  "from_name": "Example Team",
  "items": [
    {
      "email_id": "00000000-0000-0000-0000-000000000002",
      "tracking_token": "00000000-0000-0000-0000-000000000003",
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

**Example curl request:**

```bash
curl -X POST http://127.0.0.1:8001/internal/generate \
  -H "Content-Type: application/json" \
  -H "X-Internal-Token: YOUR_INTERNAL_TOKEN" \
  -d '{
    "batch_id": "00000000-0000-0000-0000-000000000001",
    "from_email": "sender@example.com",
    "from_name": "Example Team",
    "items": [{
      "email_id": "00000000-0000-0000-0000-000000000002",
      "tracking_token": "00000000-0000-0000-0000-000000000003",
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

Replace the example UUIDs with actual IDs from PostgreSQL and `YOUR_INTERNAL_TOKEN` with your configured internal token.

The email record must belong to the specified batch and have `accepted` status.

### 6. Understand the response

A successful request returns HTTP `202 Accepted`, for example:

```json
{
  "batch_id": "00000000-0000-0000-0000-000000000001",
  "received": 1
}
```

**Important:** HTTP 202 means the request has been accepted for background processing. It does not guarantee that the email has been published to RabbitMQ or delivered.

The Generation API performs these operations:

1. Loads and locks the accepted email record from PostgreSQL.
2. Retrieves its template and variables.
3. Renders the subject, HTML, and plain-text content.
4. Publishes the generated message to RabbitMQ.
5. Waits for RabbitMQ publisher confirmation.
6. Updates the email status to `queued` after successful publication.

Part C consumes the queued message and handles actual email delivery.

### 7. Preview generated email content

Use the preview endpoint to render sample email content without publishing messages to RabbitMQ.

```text
POST /internal/generate/preview
```

Refer to `/docs` for the preview request schema.

### 8. Important behavior

- Maximum request size: 1000 items by default.
- Only emails with `accepted` status are processed.
- Invalid templates or rendering errors can cause an email to be marked `failed`.
- RabbitMQ publication failures do not mark an email as `queued`.
- HTTP `202` confirms request acceptance, not final publication or delivery.
- Queue capacity and backpressure are handled separately by the surrounding system.
- Actual SMTP/KumoMTA delivery is handled by Part C.
















# Generation API (Part B)

## 1. Overview

The Generation API is Part B of the Email API System. It is responsible for generating personalized email content and publishing messages to RabbitMQ for asynchronous delivery.

It receives a generation request from the Ingestion API (Part A), retrieves the corresponding accepted email records and templates from PostgreSQL, renders personalized email content, and publishes the generated messages to RabbitMQ.

The Sender Worker (Part C) consumes these messages and handles the actual email delivery.

The Generation API does not directly send emails through SMTP or KumoMTA.

### Main responsibilities

- Authenticate internal service requests.
- Validate generation requests.
- Retrieve accepted email records from PostgreSQL.
- Retrieve email templates and required variables.
- Render personalized subject, HTML, and plain-text content.
- Generate stable Message-ID values.
- Publish messages to RabbitMQ.
- Wait for RabbitMQ publisher confirmations.
- Update email records after successful publication.
- Expose health and readiness endpoints.

---

## 2. Position in the System Architecture

The system consists of three application services and supporting infrastructure.

```text
                  CLIENT
                    |
                    v
          +---------------------+
          |   Ingestion API     |
          |       Part A        |
          +---------------------+
                    |
                    | Internal HTTP request
                    v
          +---------------------+
          |   Generation API    |
          |       Part B        |
          +---------------------+
                    |
             +------+------+
             |             |
             v             v
       +-----------+  +-------------+
       | PostgreSQL|  |  RabbitMQ   |
       |           |  |             |
       +-----------+  +-------------+
                           |
                           | email.exchange
                           | routing key: send
                           v
                   +------------------+
                   |  Sender Worker   |
                   |      Part C      |
                   +------------------+
                           |
                           v
                      SMTP / MTA
                           |
                           v
                    Recipient Server
```

### Service responsibilities

| Component | Responsibility |
|---|---|
| Ingestion API (Part A) | Accepts bulk requests, validates recipients, creates batches and email records, and checks queue capacity. |
| Generation API (Part B) | Retrieves email data, renders personalized content, and publishes messages to RabbitMQ. |
| Sender Worker (Part C) | Consumes generated messages and handles email delivery, retries, and delivery status updates. |
| PostgreSQL | Stores accounts, batches, emails, templates, and processing information. |
| RabbitMQ | Provides asynchronous communication between Generation API and Sender Worker. |

### Processing sequence

1. A client submits a bulk email request to Part A.
2. Part A validates the request and stores the batch and email records in PostgreSQL.
3. Part A checks RabbitMQ queue capacity before calling Part B.
4. Part A sends an authenticated HTTP request to `POST /internal/generate`.
5. Part B validates the request and schedules batch processing.
6. Part B retrieves each email record and its associated account and template from PostgreSQL.
7. Part B renders the personalized email content.
8. Part B publishes the generated message to RabbitMQ.
9. RabbitMQ confirms successful publication.
10. Part B updates the corresponding email record to `queued`.
11. Part C consumes the message and handles delivery.

---

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.12 | Application runtime |
| FastAPI | HTTP API framework |
| Pydantic | Request validation and response schemas |
| PostgreSQL | Persistent email and template data |
| asyncpg | Asynchronous PostgreSQL access |
| RabbitMQ | Message broker |
| aio-pika | Asynchronous RabbitMQ client |
| Jinja2 | Email template rendering |
| Docker | Containerization |
| Uvicorn | ASGI application server |

---

## 4. Project Structure
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

### Generation API – Project Structure and File Responsibilities

| File | Responsibility |
|---|---|
| `Dockerfile` | Builds the Generation API container using Python 3.12. It installs the required dependencies, copies the application code, creates a non-root application user, and starts the FastAPI application using Uvicorn on port `8001`. |
| `requirements.txt` | Defines the Python dependencies required by the service, including FastAPI, Uvicorn, asyncpg, aio-pika, Jinja2, and Pydantic with email validation support. |
| `app/__init__.py` | Marks the `app` directory as a Python package so its modules can be imported by the application. |
| `app/main.py` | Creates the FastAPI application and manages its startup and shutdown lifecycle. It reads the operating mode and initializes PostgreSQL and RabbitMQ connections when running in integrated mode. |
| `app/config.py` | Loads and validates environment configuration, including database connection settings, RabbitMQ credentials and connection details, the internal authentication token, worker concurrency, publishing rate, and maximum batch size. |
| `app/schemas.py` | Defines the API request and response models using Pydantic. It validates UUIDs, email addresses, required fields, item limits, and duplicate email IDs or tracking tokens within a single request. |
| `app/db.py` | Manages PostgreSQL connection pooling and database operations. It retrieves email and template records, locks email rows during processing, and updates email statuses such as `queued` and `failed`. |
| `app/rendering.py` | Uses Jinja2 to generate personalized email subjects, HTML content, and plain-text content. It validates required template variables and supported URL variables before rendering. |
| `app/generation_engine.py` | Provides the in-memory rendering engine and sample templates used by the standalone preview functionality. These preview templates are separate from the PostgreSQL-backed templates used during integrated processing. |
| `app/publisher.py` | Manages RabbitMQ publishing. It declares the exchange and publishes persistent JSON messages with publisher confirmations and mandatory routing enabled. |
| `app/processor.py` | Coordinates the main email-generation workflow. It retrieves accepted email records, loads templates, renders personalized content, applies concurrency and publishing-rate limits, publishes messages to RabbitMQ, and updates database statuses. |
| `app/routes/__init__.py` | Marks the `routes` directory as a Python package. |
| `app/routes/generate.py` | Implements the email-generation and preview endpoints. It handles internal-token authentication, request validation, item limits, and background batch processing. |
| `app/routes/health.py` | Implements the service liveness and readiness endpoints. In integrated mode, readiness checks the availability of PostgreSQL and RabbitMQ. |
---

## 5. Application Operating Modes

The Generation API supports two operating modes.

### 5.1 Standalone mode

Standalone mode is intended for independent development and preview testing.

Characteristics:

- Does not require PostgreSQL connections.
- Does not require RabbitMQ connections.
- Uses in-memory templates defined in `generation_engine.py`.
- Supports email preview generation.
- Does not publish messages.
- The integrated generation endpoint returns HTTP 503 because the processor is not initialized.

Configuration:

```env
GENERATION_MODE=standalone
INTERNAL_TOKEN=your_internal_token
```

### 5.2 Integrated mode

Integrated mode is used when the service is connected to the complete email processing system.

Characteristics:

- Connects to PostgreSQL.
- Connects to RabbitMQ.
- Initializes the database connection pool.
- Initializes the RabbitMQ publisher.
- Creates the GenerationProcessor.
- Retrieves templates and email records from PostgreSQL.
- Publishes generated messages to RabbitMQ.

Configuration:

```env
GENERATION_MODE=integrated
```

The integrated mode requires valid PostgreSQL, RabbitMQ, and internal authentication configuration.

---

## 6. API Endpoints

The Generation API exposes four endpoints.

| Method | Endpoint | Purpose |
|---|---|---|
| POST | `/internal/generate` | Processes a batch and schedules generation. |
| POST | `/internal/generate/preview` | Generates preview content without database or RabbitMQ access. |
| GET | `/health` | Checks whether the API process is running. |
| GET | `/health/ready` | Checks whether the service is ready to handle requests. |

### 6.1 Generate emails

**Endpoint**

```http
POST /internal/generate
```

**Authentication**

```http
X-Internal-Token: your_internal_token
Content-Type: application/json
```

**Example request**

```json
{
  "batch_id": "11111111-1111-4111-8111-111111111111",
  "from_email": "no-reply@example.com",
  "from_name": "Example Team",
  "items": [
    {
      "email_id": "22222222-2222-4222-8222-222222222222",
      "tracking_token": "33333333-3333-4333-8333-333333333333",
      "template_id": "otp_email",
      "to_email": "asha@example.com",
      "to_name": "Asha",
      "variables": {
        "name": "Asha",
        "otp": "482913"
      }
    }
  ]
}
```

**Successful response**

HTTP 202 Accepted

```json
{
  "batch_id": "11111111-1111-4111-8111-111111111111",
  "received": 1
}
```

HTTP 202 means the request was accepted and batch processing was scheduled. It does not mean every email has already been published to RabbitMQ or delivered.

### 6.2 Preview generation

**Endpoint**

```http
POST /internal/generate/preview
```

This endpoint renders preview content using the in-memory templates.

It does not retrieve email records from PostgreSQL or publish messages to RabbitMQ.

It is useful for independently testing template rendering.

The response contains the generated email details, including:

- Email ID
- Batch ID
- Template ID
- Recipient
- Sender
- Subject
- HTML body
- Plain-text body
- Message-ID
- Tracking token

### 6.3 Health endpoint

**Endpoint**

```http
GET /health
```

Successful response:

```json
{
  "status": "ok"
}
```

This endpoint checks whether the API process is running. It does not verify database or RabbitMQ connectivity.

### 6.4 Readiness endpoint

**Endpoint**

```http
GET /health/ready
```

In standalone mode:

```json
{
  "status": "ready",
  "mode": "standalone",
  "database": "not_required",
  "rabbitmq": "not_required"
}
```

In integrated mode, the endpoint checks PostgreSQL and RabbitMQ connectivity.

A successful response has HTTP 200:

```json
{
  "status": "ready",
  "database": true,
  "rabbitmq": true
}
```

If either dependency is unavailable, the endpoint returns HTTP 503 with `not_ready`.

### 6.5 Authentication and validation errors

The internal endpoints use the `X-Internal-Token` header.

The token is compared using `hmac.compare_digest`.

Common responses include:

| HTTP status | Meaning |
|---|---|
| 200 | Preview or health request completed successfully. |
| 202 | Generation request accepted for background processing. |
| 401 | Missing or invalid internal token. |
| 413 | Request exceeds the configured maximum number of items. |
| 422 | Request body failed Pydantic validation. |
| 503 | Integrated processing is unavailable, or the service is not ready. |

---

## 7. Request Validation

The request schemas are defined in `schemas.py`.

### GenerateItem

Each item contains:

| Field | Type | Validation |
|---|---|---|
| `email_id` | UUID | Required |
| `tracking_token` | UUID | Required |
| `template_id` | String | Required, 1–128 characters |
| `to_email` | Email | Required, valid email address |
| `to_name` | String or null | Optional, maximum 256 characters |
| `variables` | Object | Optional, defaults to an empty object |

Extra fields are forbidden.

### GenerateRequest

| Field | Type | Validation |
|---|---|---|
| `batch_id` | UUID | Required |
| `from_email` | Email | Required |
| `from_name` | String or null | Optional |
| `items` | Array | At least 1 item, maximum 1000 items by schema default |

Duplicate email IDs and duplicate tracking tokens within a request are rejected.

The configurable `GENERATION_MAX_ITEMS` setting is also checked by the route.

---

## 8. PostgreSQL Integration

The database implementation is in `db.py`.

PostgreSQL is the source of truth for integrated email generation.

The processor retrieves the actual email information from the database rather than relying on all the corresponding values in the incoming request.

### Main database entities used

#### accounts

Stores sender account information.

The Generation API uses the account's sender email address and sender name.

#### batches

Stores batch-level information, including:

- Batch ID
- Account ID
- Total count
- Accepted count
- Rejected count
- Creation timestamp

#### emails

Stores individual recipient records and processing status.

Important fields include:

- Email ID
- Batch ID
- Position
- Template ID
- Recipient email
- Recipient name
- Template variables
- Status
- Reject reason
- Attempts
- Last error
- Message-ID
- Tracking token
- Queued timestamp
- Sent timestamp
- Failed timestamp

#### templates

Stores email templates, including:

- Template ID
- Subject template
- HTML template
- Text template
- Required variables

### Email processing status

The Generation API expects an email to have the status:

```text
accepted
```

Only accepted email records are eligible for generation.

After successful RabbitMQ publication and confirmation, the record is updated to:

```text
queued
```

If a permanent template or rendering validation error occurs, the record can be marked:

```text
failed
```

Records in other statuses are skipped.

### Transaction and row locking

Each email is processed inside a PostgreSQL transaction.

The processor retrieves the email using a row-level lock.

This prevents concurrent Generation API processing attempts from simultaneously processing the same accepted email row.

The transaction includes:

1. Retrieving and locking the email.
2. Validating batch ownership and status.
3. Retrieving the account and template data.
4. Rendering the email.
5. Publishing the message to RabbitMQ.
6. Updating the database status to `queued`.

The database update is performed only after RabbitMQ confirms publication.

If publication fails, the transaction rolls back and the email remains eligible for a later retry.

---

## 9. Email Template Rendering

Template rendering is implemented in `rendering.py`.

The Generation API uses Jinja2 with a sandboxed environment.

### Rendering capabilities

- Personalized subject generation.
- Personalized HTML body generation.
- Personalized plain-text body generation.
- Required-variable validation.
- HTML autoescaping.
- Undefined-variable detection.
- URL validation for recognized URL variables.

### Example template

Subject:

```text
Hello {{ name }}, your OTP is ready
```

HTML:

```html
<html>
<body>
    <h2>Hello {{ name }}</h2>
    <p>Your OTP is {{ otp }}</p>
</body>
</html>
```

Variables:

```json
{
  "name": "Asha",
  "otp": "482913"
}
```

Rendered subject:

```text
Hello Asha, your OTP is ready
```

Rendered HTML:

```html
<html>
<body>
    <h2>Hello Asha</h2>
    <p>Your OTP is 482913</p>
</body>
</html>
```

### Required variables

The required variables are stored with each template.

If a required variable is missing, rendering fails and the email is marked as failed.

### HTML and text rendering

HTML templates are rendered with autoescaping enabled to reduce HTML injection risks.

Subject and plain-text templates are rendered without HTML autoescaping.

If a text template is not supplied, the renderer can derive plain text from the generated HTML.

### URL validation

Recognized URL variables include:

- `reset_link`
- `dashboard_url`
- `order_url`
- `offer_url`
- `terms_url`
- `unsubscribe_url`

These variables must contain absolute HTTP or HTTPS URLs and must not contain credentials or whitespace.

---

## 10. RabbitMQ Integration

RabbitMQ publishing is implemented in `publisher.py`.

The Generation API publishes generated email messages to a durable direct exchange.

### Exchange configuration

| Property | Value |
|---|---|
| Exchange | `email.exchange` |
| Exchange type | Direct |
| Durable | Yes |
| Routing key | `send` |
| Queue | `email.send` |

The Sender Worker consumes messages from the configured queue.

### Message format

The Generation API publishes one JSON message per email.

Example:

```json
{
  "email_id": "22222222-2222-4222-8222-222222222222",
  "batch_id": "11111111-1111-4111-8111-111111111111",
  "to_email": "asha@example.com",
  "to_name": "Asha",
  "from_email": "no-reply@example.com",
  "from_name": "Example Team",
  "subject": "Your One-Time Password",
  "html": "<html><body><h2>Hello Asha</h2></body></html>",
  "text": "Hello Asha",
  "message_id": "<22222222-2222-4222-8222-222222222222@example.com>",
  "tracking_token": "33333333-3333-4333-8333-333333333333"
}
```

### Message properties

Messages are published with:

| Property | Value |
|---|---|
| Content type | `application/json` |
| Content encoding | `utf-8` |
| Delivery mode | Persistent (2) |
| Mandatory | `true` |
| Publisher confirms | Enabled |

### Message headers

```text
schema_version = 1
producer = generation-api
x-attempt = 1
```

### Publisher confirmation

The publisher waits for RabbitMQ confirmation.

The database status is changed to `queued` only after successful publication confirmation.

If RabbitMQ rejects the publication or the publisher encounters an error, the transaction is rolled back.

### Stable Message-ID

The Generation API generates a deterministic Message-ID using the email UUID and sender domain.

Example:

```text
<email-uuid@example.com>
```

This value remains stable across generation retries for the same email record.

The Sender Worker can use this identifier as part of its duplicate-detection strategy.

A stable Message-ID alone does not guarantee exactly-once processing.

---

## 11. Integration Between Part A, Part B and Part C

### Part A to Part B

Part A calls:

```http
POST /internal/generate
```

It supplies:

- Batch ID
- Sender details
- Email IDs
- Tracking tokens
- Template IDs
- Recipient details
- Template variables

The request must include the internal authentication token.

Part B then uses the email IDs and batch ID to retrieve the authoritative records from PostgreSQL.

### Part B to Part C

Part B publishes one generated JSON message for each successfully processed email.

Part C receives the message from RabbitMQ.

The generated message contains the recipient, sender, rendered content, Message-ID, batch ID, email ID and tracking token.

Part C is responsible for consuming the message and handling delivery.

### Responsibility boundaries

| Operation | Responsible component |
|---|---|
| Bulk request validation | Part A |
| Batch and recipient record creation | Part A |
| Initial queue-capacity check | Part A |
| Email content generation | Part B |
| Template rendering | Part B |
| RabbitMQ message publication | Part B |
| SMTP/KumoMTA delivery | Part C |
| Delivery retry handling | Part C |
| Final delivery status | Part C |

---

## 12. Configuration

The Generation API reads its configuration from environment variables.

The main configuration implementation is in `config.py`.

### Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `GENERATION_MODE` | Operating mode | `standalone` |
| `INTERNAL_TOKEN` | Internal API authentication | Required |
| `DATABASE_URL` | Complete PostgreSQL URL | Optional if individual DB variables are supplied |
| `POSTGRES_USER` | PostgreSQL username | Required when building the URL |
| `POSTGRES_PASSWORD` | PostgreSQL password | Required when building the URL |
| `POSTGRES_DB` | PostgreSQL database name | Required when building the URL |
| `POSTGRES_HOST` | PostgreSQL hostname | `postgres` |
| `POSTGRES_PORT` | PostgreSQL port | `5432` |
| `RABBITMQ_URL` | Complete RabbitMQ URL | Optional if individual RabbitMQ variables are supplied |
| `RABBITMQ_USER` | RabbitMQ username | Required when building the URL |
| `RABBITMQ_PASSWORD` | RabbitMQ password | Required when building the URL |
| `RABBITMQ_HOST` | RabbitMQ hostname | `rabbitmq` |
| `RABBITMQ_PORT` | RabbitMQ port | `5672` |
| `RABBITMQ_VHOST` | RabbitMQ virtual host | `/` |
| `GENERATION_CONCURRENCY` | Maximum concurrent email processing workers | `10` |
| `PUBLISH_RATE_PER_SECOND` | Maximum publication rate per process | `50` |
| `GENERATION_MAX_ITEMS` | Maximum items accepted per request | `1000` |
| `LOG_LEVEL` | Application logging level | `INFO` |

Positive integer settings are validated during configuration loading.

Passwords and tokens must be supplied through environment variables, not hardcoded in application source files.

---

## 13. Docker Setup

The Generation API uses Python 3.12 slim as its base image.

The Dockerfile:

1. Sets the working directory to `/app`.
2. Installs Python dependencies.
3. Copies the application source.
4. Creates a non-root application user.
5. Runs Uvicorn on port 8001.

Container startup command:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8001
```

### Build the service

From the repository root:

```bash
docker compose build generation-api
```

### Start the service

```bash
docker compose up -d generation-api
```

### View logs

```bash
docker compose logs -f generation-api
```

### Check container status

```bash
docker compose ps generation-api
```

### Check liveness

```bash
curl -i http://127.0.0.1:8001/health
```

### Check readiness

```bash
curl -i http://127.0.0.1:8001/health/ready
```

### Stop the service

```bash
docker compose stop generation-api
```

The local Compose override maps the Generation API to `127.0.0.1:8001`, restricting host access to the local machine.

Docker-internal services communicate using their Compose service names and container ports.

---

## 14. Concurrency and Throughput

The Generation API uses bounded asynchronous processing.

### Concurrency control

The configured `GENERATION_CONCURRENCY` setting controls the maximum number of email-processing workers.

The default is:

```env
GENERATION_CONCURRENCY=10
```

The processor does not create one independent asyncio task for every recipient.

Instead, it creates a bounded asyncio queue and a limited number of worker tasks.

The queue size is based on twice the worker count, with a minimum of one item.

This helps control memory usage when processing large batches.

### Publication rate limiting

The `PUBLISH_RATE_PER_SECOND` setting controls the publication rate.

Default:

```env
PUBLISH_RATE_PER_SECOND=50
```

A shared asynchronous rate limiter spaces out RabbitMQ publications across the workers in one application process.

This is a per-process limit, not a global limit across multiple Generation API replicas.

### Maximum request size

The default maximum request size is:

```env
GENERATION_MAX_ITEMS=1000
```

This is an API request limit.

It is separate from the RabbitMQ queue capacity.

---

## 15. Queue Capacity and Backpressure

RabbitMQ queue capacity is an infrastructure-level limit.

The local project configures the `email.send` queue with:

```text
x-max-length = 1000
x-overflow = reject-publish
```

This means RabbitMQ rejects new publications when the queue reaches its configured maximum.

Part A performs an initial queue-capacity check before calling Part B.

This helps avoid submitting new batches when the queue is already full.

However, queue capacity can change between the check and the actual publication. Therefore, Part B also handles RabbitMQ publication failures.

### When the queue is full

1. Part B attempts to publish a generated message.
2. RabbitMQ rejects the publication when the configured limit is reached.
3. The publisher raises an error.
4. The PostgreSQL transaction rolls back.
5. The email remains `accepted`.
6. The email is not marked `queued`.

This prevents an email from being marked queued when RabbitMQ has not confirmed its publication.

### Important distinction

The following limits are different:

| Limit | Meaning |
|---|---|
| `GENERATION_MAX_ITEMS` | Maximum recipients in one Generation API request. |
| RabbitMQ `x-max-length` | Maximum ready messages allowed in the configured queue. |
| `GENERATION_CONCURRENCY` | Number of email-processing workers in one process. |
| `PUBLISH_RATE_PER_SECOND` | Maximum publication rate in one process. |

Increasing the request limit does not automatically increase RabbitMQ capacity.

---

## 16. Error Handling and Recovery

The Generation API handles different failure categories.

### Authentication failure

If the internal token is missing or invalid, the request is rejected with HTTP 401.

### Request validation failure

If required fields, UUIDs, email addresses, duplicate identifiers or item limits are invalid, Pydantic or route validation rejects the request.

### Missing email record

If an email ID cannot be found in PostgreSQL, the processor logs the issue and skips that item.

### Batch mismatch

If an email does not belong to the requested batch, it is skipped.

### Incorrect email status

Only records with status `accepted` are processed.

Other statuses are skipped to avoid generating already processed emails.

### Unknown template

If the template does not exist in PostgreSQL, the email is marked `failed`.

### Rendering failure

If required variables are missing or template rendering fails validation, the email is marked `failed`.

### Invalid sender address

If the sender address retrieved from the account record is invalid, the email is marked `failed`.

### RabbitMQ publication failure

If RabbitMQ does not confirm publication, the database transaction rolls back.

The email remains `accepted`.

### PostgreSQL failure

Unexpected database or infrastructure errors are logged. The transaction is rolled back rather than incorrectly marking the email as failed.

### Recovery considerations

The current Generation API does not contain an independent periodic polling mechanism that automatically discovers and resubmits all accepted email records.

A later retry or resubmission mechanism must be coordinated with Part A or an operational recovery process.

---

## 17. Delivery Guarantees and Limitations

The Generation API confirms publication, not final email delivery.

The following states must be distinguished:

| Status | Meaning |
|---|---|
| `accepted` | Email record is accepted and eligible for generation. |
| `queued` | RabbitMQ confirmed publication and the database was updated. |
| `sent` | Delivery status managed by the downstream sender component. |
| `failed` | Email processing or delivery failure recorded by the responsible component. |

### Important transaction limitation

PostgreSQL and RabbitMQ do not share a single distributed transaction in this implementation.

A possible failure scenario is:

1. RabbitMQ confirms publication.
2. The application encounters a PostgreSQL commit failure.
3. The database transaction rolls back.
4. The message may already exist in RabbitMQ.
5. A later retry could publish the same logical email again.

The stable Message-ID helps downstream duplicate detection, but exactly-once publication or delivery is not guaranteed.

An idempotent consumer or transactional outbox design would provide stronger recovery guarantees.

---

## 18. Security Considerations

The Generation API implements several security measures:

- Internal API authentication using a shared token.
- Constant-time token comparison using `hmac.compare_digest`.
- Pydantic request validation.
- Strict template rendering with Jinja2 sandboxing.
- HTML autoescaping.
- Required-variable validation.
- Validation of recognized URL variables.
- Non-root Docker container execution.
- Environment-based database and RabbitMQ credentials.
- Local host port bindings restricted to loopback in the development override.

### Operational recommendations

- Use strong, unique internal tokens.
- Rotate credentials if they are exposed.
- Do not commit `.env` files.
- Do not use development credentials in production.
- Restrict PostgreSQL and RabbitMQ access.
- Use TLS for service communication where required by deployment architecture.
- Avoid logging authentication tokens or sensitive recipient data.
- Apply appropriate access control to internal endpoints.

---

## 19. Verification Performed

The following local checks and manual tests have been performed during development.

| Test | Observed result |
|---|---|
| Single-recipient generation | Successfully rendered and published. |
| PostgreSQL status update | Email status changed to `queued` after confirmed publication. |
| RabbitMQ message inspection | Generated JSON payload and message metadata were verified. |
| Five-recipient batch | All five records reached `queued`. |
| Fifty-recipient batch | All fifty records reached `queued`. |
| One-thousand-recipient batch | Request was accepted; 294 records reached `queued` before the queue reached capacity. |
| Queue-full behaviour | Further publications were rejected; affected database transactions rolled back and records remained `accepted`. |

The one-thousand-recipient test demonstrated that the API can process a large request and handle queue rejection without incorrectly marking rejected publications as queued.

The test did not establish production throughput or end-to-end delivery performance.

The Sender Worker and actual SMTP/KumoMTA delivery path are outside the scope of these Generation API tests.

---

## 20. Known Limitations

Current implementation limitations include:

1. No independent periodic recovery worker for accepted emails left after infrastructure failures.
2. No distributed transaction spanning PostgreSQL and RabbitMQ.
3. Exactly-once message publication and delivery are not guaranteed.
4. The publication rate limit is per application process.
5. Queue capacity is configured separately in RabbitMQ.
6. Preview templates are in-memory and are separate from integrated PostgreSQL templates.
7. HTTP 202 indicates scheduled processing, not completion of the entire batch.
8. Production-scale throughput and end-to-end delivery require separate load and integration testing.

---

## 21. Future Improvements

Potential improvements include:

- Implement a transactional outbox for reliable database-to-broker publication.
- Add an explicit retry scheduler for accepted records that remain unprocessed.
- Add idempotency and deduplication guarantees across service boundaries.
- Add batch-level processing summaries and progress endpoints.
- Add metrics for rendering latency, publication latency, failures and queue pressure.
- Add structured tracing across Parts A, B and C.
- Add automated unit and integration tests for failure scenarios.
- Add configurable graceful shutdown for in-flight batches.
- Evaluate global rate limiting when running multiple Generation API replicas.
- Add production load testing and capacity planning.

---

## 22. Summary

The Generation API is the content-generation and message-publication layer of the Email API System.

It receives validated batch requests from Part A, retrieves authoritative email and template data from PostgreSQL, generates personalized subject, HTML and plain-text content, and publishes persistent JSON messages to RabbitMQ.

It uses asynchronous bounded workers, PostgreSQL row locking, transaction management, publication rate limiting and RabbitMQ publisher confirmations to control processing and handle failures.

The Sender Worker (Part C) is responsible for consuming generated messages and carrying out actual email delivery.

The Generation API therefore provides the bridge between accepted email records and asynchronous email delivery.

# Email API System

A containerized email delivery platform designed to handle bulk email ingestion, content generation, asynchronous message processing, and email delivery through KumoMTA.

## Architecture

The system consists of three independent services:

- **Ingestion API (Part A):** Accepts bulk email requests, validates input, handles authentication, capacity management, and idempotency.
- **Generation API (Part B):** Generates personalized email content using templates and publishes messages to RabbitMQ.
- **Sender Worker (Part C):** Consumes messages from RabbitMQ and delivers emails through KumoMTA.

### Infrastructure

- **PostgreSQL:** Stores accounts, campaigns, templates, email records, and processing information.
- **RabbitMQ:** Handles asynchronous message processing and communication between services.
- **Docker Compose:** Manages the application services and infrastructure.

## Project Structure

```text
email-api-system/
├── db/                 # Database schema and seed files
├── infra/              # Infrastructure configuration
├── services/
│   ├── ingestion-api/  # Part A
│   ├── generation-api/ # Part B
│   └── sender-worker/  # Part C
├── tools/              # Development and testing utilities
├── tests/              # Integration and load tests
└── docs/               # Project documentation
```

## Getting Started

### Prerequisites

- Docker
- Docker Compose
- Git

### Setup

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd email-api-system
   ```

2. Create your local environment file:

   ```bash
   cp .env.example .env
   ```

3. Configure the required environment variables in `.env`.

4. Start the application:

   ```bash
   docker compose up --build
   ```

## Development

Each service maintains its own dependencies, Dockerfile, application code, and tests.

Refer to `docs/implementation-plan.md` for the implementation details.

## Security

- Never commit `.env` files or credentials.
- Use `.env.example` to document required environment variables.
- Protect API endpoints using authentication.
- Use appropriate access controls for database and message broker connections.

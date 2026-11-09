# Local Development Port Allocation

This project runs alongside the separate mini-esp project.
The host ports below avoid conflicts with mini-esp.

| Service | Host port | Container port |
|---|---:|---:|
| PostgreSQL | 5433 | 5432 |
| RabbitMQ AMQP | 5672 | 5672 |
| RabbitMQ Management UI | 15672 | 15672 |
| Ingestion API | 8000 | 8000 |
| Generation API | 8001 | 8001 |
| Mailpit SMTP | 1026 | 1025 |
| Mailpit Web UI | 8026 | 8025 |

Host ports for PostgreSQL and Mailpit are bound to 127.0.0.1.

Docker-internal connections remain unchanged:
- PostgreSQL: postgres:5432
- RabbitMQ: rabbitmq:5672
- Mailpit SMTP: mailpit:1025

The mini-esp project retains its existing ports:
- PostgreSQL: 5432
- Mailpit SMTP: 1025
- Mailpit Web UI: 8025

Do not change the internal container ports when changing host mappings.

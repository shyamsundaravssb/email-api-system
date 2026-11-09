import json
import os
import time
import uuid
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import psycopg2


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]

API_URL = "http://127.0.0.1:8001/internal/generate"
RECIPIENT_COUNT = 1000

ACCOUNT_ID = "f56acb23-c62a-45bc-b1a5-d73f13b853bd"

FROM_EMAIL = os.getenv(
    "TEST_FROM_EMAIL",
    "no-reply@example.com"
)

FROM_NAME = "Tracking Test"

TEMPLATE_ID = "password_reset"

POLL_INTERVAL = 3
MAX_WAIT_SECONDS = 180


# --------------------------------------------------
# LOAD .ENV
# --------------------------------------------------

def load_env():
    env_file = ROOT / ".env"

    if not env_file.exists():
        raise RuntimeError(".env file not found")

    for line in env_file.read_text().splitlines():
        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value


load_env()

INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN")

if not INTERNAL_TOKEN:
    raise RuntimeError("INTERNAL_TOKEN is missing from .env")


# --------------------------------------------------
# DATABASE CONNECTION
# --------------------------------------------------

def get_database_connection():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        dbname=os.getenv("POSTGRES_DB", "emailapi"),
        user=os.getenv("POSTGRES_USER", "emailapi"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )


# --------------------------------------------------
# CREATE BATCH AND EMAIL RECORDS
# --------------------------------------------------

def create_batch():
    batch_id = str(uuid.uuid4())

    connection = get_database_connection()

    try:
        with connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    INSERT INTO batches (
                        id,
                        account_id,
                        total_count,
                        accepted_count,
                        rejected_count
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        batch_id,
                        ACCOUNT_ID,
                        RECIPIENT_COUNT,
                        RECIPIENT_COUNT,
                        0,
                    ),
                )

                items = []

                for position in range(1, RECIPIENT_COUNT + 1):
                    email_id = str(uuid.uuid4())

                    recipient = (
                        f"tracking1000-{batch_id[:8]}-"
                        f"{position:04d}@example.com"
                    )

                    variables = {
                        "name": f"Tracking Test User {position}",
                        "link": "https://example.com/reset-password",
                    }

                    cursor.execute(
                        """
                        INSERT INTO emails (
                            id,
                            batch_id,
                            position,
                            template_id,
                            to_email,
                            to_name,
                            variables,
                            status
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, 'accepted'
                        )
                        RETURNING tracking_token
                        """,
                        (
                            email_id,
                            batch_id,
                            position,
                            TEMPLATE_ID,
                            recipient,
                            f"Tracking User {position}",
                            json.dumps(variables),
                        ),
                    )

                    tracking_token = str(cursor.fetchone()[0])

                    items.append({
                        "email_id": email_id,
                        "tracking_token": tracking_token,
                        "template_id": TEMPLATE_ID,
                        "to_email": recipient,
                        "to_name": f"Tracking User {position}",
                        "variables": variables,
                    })

        print(f"\nBatch created: {batch_id}")
        print(f"Recipients inserted: {len(items)}")

        return batch_id, items

    finally:
        connection.close()


# --------------------------------------------------
# SUBMIT GENERATION REQUEST
# --------------------------------------------------

def submit_generation(batch_id, items):
    payload = {
        "batch_id": batch_id,
        "from_email": FROM_EMAIL,
        "from_name": FROM_NAME,
        "items": items,
    }

    request = Request(
        API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Internal-Token": INTERNAL_TOKEN,
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=60) as response:
            response_body = response.read().decode("utf-8")

            print("\n========== GENERATION API ==========")
            print("HTTP Status:", response.status)
            print("Response:", response_body)

            if response.status != 202:
                raise RuntimeError("Generation request was not accepted")

    except HTTPError as exc:
        print("API Error:", exc.code)
        print(exc.read().decode("utf-8"))
        raise

    except URLError as exc:
        raise RuntimeError(
            f"Cannot connect to Generation API: {exc.reason}"
        ) from exc


# --------------------------------------------------
# CHECK DATABASE RESULTS
# --------------------------------------------------

def get_results(batch_id):
    connection = get_database_connection()

    try:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT status, COUNT(*)
                FROM emails
                WHERE batch_id = %s
                GROUP BY status
                ORDER BY status
                """,
                (batch_id,),
            )

            status_counts = dict(cursor.fetchall())

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM tracking_links tl
                JOIN emails e ON e.id = tl.email_id
                WHERE e.batch_id = %s
                """,
                (batch_id,),
            )

            tracking_count = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM emails
                WHERE batch_id = %s
                """,
                (batch_id,),
            )

            total_count = cursor.fetchone()[0]

        return total_count, status_counts, tracking_count

    finally:
        connection.close()


# --------------------------------------------------
# WAIT FOR TRACKING LINKS
# --------------------------------------------------

def wait_for_results(batch_id):
    print("\n========== WAITING FOR PROCESSING ==========")

    start_time = time.time()

    while time.time() - start_time < MAX_WAIT_SECONDS:
        total, statuses, tracking_count = get_results(batch_id)

        processed = statuses.get("queued", 0) + statuses.get("retrying", 0) + statuses.get("sent", 0) + statuses.get("failed", 0)

        print(
            f"Total: {total}/1000 | "
            f"Tracking links: {tracking_count} | "
            f"Statuses: {statuses}"
        )

        if tracking_count >= RECIPIENT_COUNT:
            break

        if processed >= RECIPIENT_COUNT:
            break

        time.sleep(POLL_INTERVAL)

    print("\n========== FINAL TRACKING REPORT ==========")

    total, statuses, tracking_count = get_results(batch_id)

    print("Batch ID:", batch_id)
    print("Total email records:", total)
    print("Email statuses:", statuses)
    print("Tracking links created:", tracking_count)
    print("Expected tracking links:", RECIPIENT_COUNT)

    if tracking_count == RECIPIENT_COUNT:
        print("\nSUCCESS: Tracking links created for all 1000 emails.")
    else:
        print(
            f"\nINCOMPLETE: {RECIPIENT_COUNT - tracking_count} "
            "tracking links are still missing."
        )


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():
    print("==========================================")
    print(" GENERATION API - 1000 EMAIL TRACKING TEST")
    print("==========================================")

    batch_id, items = create_batch()

    submit_generation(batch_id, items)

    wait_for_results(batch_id)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
End-to-end ingestion performance test for the Email API system.

Submits a recipient batch to the Ingestion API, then polls Mailpit until
all expected recipients are visible or the timeout is reached.

The script does not delete messages or modify the source payload file.
It assigns a unique email prefix to every run.

Usage:
    python3 test-ingestion-scaled.py

Optional environment variables:
    INGESTION_URL   Default: http://127.0.0.1:8000/batches
    MAILPIT_URL     Default: http://127.0.0.1:8026/api/v1/messages?start=0&limit=1000
    API_KEY         Default: dev-api-key
    PAYLOAD_FILE    Default: /tmp/email-api-e2e1000.json
    EXPECTED        Default: 1000
    TIMEOUT         Default: 180 seconds

Requirements:
    Python 3 standard library only.
"""

import json
import os
import time
import uuid
import urllib.request
import urllib.error
from collections import Counter

PAYLOAD_FILE = os.getenv("PAYLOAD_FILE", "/tmp/email-api-e2e1000.json")
INGESTION_URL = os.getenv(
    "INGESTION_URL", "http://127.0.0.1:8000/batches"
)
MAILPIT_URL = os.getenv(
    "MAILPIT_URL",
    "http://127.0.0.1:8026/api/v1/messages?start=0&limit=1000",
)
API_KEY = os.getenv("API_KEY", "dev-api-key")
EXPECTED = int(os.getenv("EXPECTED", "1000"))
TIMEOUT = int(os.getenv("TIMEOUT", "180"))


def main():
    with open(PAYLOAD_FILE, "r", encoding="utf-8") as file:
        payload = json.load(file)

    if not isinstance(payload.get("recipients"), list):
        raise SystemExit("ERROR: Payload must contain a 'recipients' list.")

    if len(payload["recipients"]) != EXPECTED:
        raise SystemExit(
            f"ERROR: Expected {EXPECTED} recipients in the payload, "
            f"but found {len(payload['recipients'])}."
        )

    prefix = f"scaled-ingestion-{uuid.uuid4().hex[:10]}"

    for index, recipient in enumerate(payload["recipients"], start=1):
        recipient["to_email"] = f"{prefix}-{index:04d}@example.com"

    expected_emails = {
        recipient["to_email"].lower()
        for recipient in payload["recipients"]
    }

    print("Starting scaled Ingestion API E2E test")
    print("Unique prefix:", prefix)
    print("Recipients:", len(expected_emails))

    start_time = time.perf_counter()

    request = urllib.request.Request(
        INGESTION_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-API-Key": API_KEY,
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response_body = json.loads(response.read().decode("utf-8"))
            status_code = response.status

    except urllib.error.HTTPError as error:
        print("Ingestion API HTTP error:", error.code)
        print(error.read().decode("utf-8", errors="replace"))
        raise SystemExit(1)
    except urllib.error.URLError as error:
        print("Could not connect to Ingestion API:", error)
        raise SystemExit(1)

    print("Ingestion HTTP status:", status_code)
    print("Batch ID:", response_body.get("batch_id"))

    if status_code != 202:
        print("Unexpected response:", response_body)
        raise SystemExit(1)

    delivered_counts = Counter()
    deadline = time.perf_counter() + TIMEOUT

    while time.perf_counter() < deadline:
        mailpit_request = urllib.request.Request(MAILPIT_URL)

        try:
            with urllib.request.urlopen(mailpit_request, timeout=10) as response:
                mailpit = json.loads(response.read().decode("utf-8"))

        except Exception as error:
            print("Mailpit polling error:", error)
            time.sleep(1)
            continue

        # Rebuild counts from the current Mailpit snapshot on each poll.
        # This prevents the same message being counted repeatedly across polls.
        current_counts = Counter()

        for message in mailpit.get("messages", []):
            for recipient in message.get("To") or []:
                address = recipient.get("Address", "").lower()

                if address in expected_emails:
                    current_counts[address] += 1

        delivered_counts = current_counts

        if len(delivered_counts) >= EXPECTED:
            break

        time.sleep(0.25)

    elapsed = time.perf_counter() - start_time

    delivered_emails = set(delivered_counts)
    missing = expected_emails - delivered_emails
    duplicates = {
        email: count
        for email, count in delivered_counts.items()
        if count > 1
    }

    delivered = len(delivered_emails)

    print("\n========== PERFORMANCE RESULTS ==========")
    print("Expected:", EXPECTED)
    print("Delivered:", delivered)
    print("Missing:", len(missing))
    print("Duplicate recipients:", len(duplicates))
    print("Elapsed seconds:", round(elapsed, 3))
    print(
        "Throughput:",
        round(delivered / elapsed, 2) if elapsed else 0,
        "emails/sec",
    )

    if missing:
        print("First missing recipients:", list(missing)[:10])

    if duplicates:
        print("Duplicate examples:", list(duplicates.items())[:10])

    print("=========================================")

    if delivered == EXPECTED and not missing and not duplicates:
        print("RESULT: PASSED")
        return 0

    print("RESULT: INCOMPLETE OR DUPLICATES DETECTED")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
#!/usr/bin/env python3

"""
Email API performance tester.

Uses only Python standard-library modules.
Reads EMAIL_API_KEY from the project .env file.
Does not modify or delete existing database records or Mailpit messages.
"""

import argparse
import concurrent.futures
import csv
import json
import os
import re
import random
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = ROOT / ".env"

API_URL = "http://127.0.0.1:8000"
MAILPIT_URL = "http://127.0.0.1:8026"

TEMPLATES = {
    "welcome_email": lambda i: {
        "name": f"Load Test User {i}"
    },
    "otp_email": lambda i: {
        "name": f"Load Test User {i}",
        "otp": f"{random.randint(100000, 999999)}"
    },
    "password_reset": lambda i: {
        "name": f"Load Test User {i}",
        "reset_link": f"https://example.com/reset/{uuid.uuid4().hex}"
    },
}


def load_env():
    """Load simple KEY=VALUE entries without printing their values."""
    if not ENV_FILE.exists():
        raise RuntimeError(".env file was not found.")

    values = {}

    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]

        values[key] = value

    return values


class QueueOccupancyMonitor:
    """Periodically sample global and current-run live queue occupancy."""

    LIVE_STATUSES = ("accepted", "queued", "retrying")

    def __init__(self, run_id, capacity, interval_seconds=0.5):
        self.run_id = run_id
        self.capacity = capacity
        self.interval_seconds = interval_seconds
        self.samples = []
        self.errors = []
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.thread = threading.Thread(
            target=self._run,
            name="queue-occupancy-monitor",
            daemon=True,
        )

    def _sample(self):
        sql = f"""
            SELECT
                COUNT(*) FILTER (
                    WHERE status IN ('accepted', 'queued', 'retrying')
                )::text
                || '|'
                ||
                COUNT(*) FILTER (
                    WHERE status IN ('accepted', 'queued', 'retrying')
                      AND to_email LIKE 'loadtest-{self.run_id}-%@example.com'
                )::text
            FROM emails;
        """

        command = [
            "docker", "compose", "exec", "-T", "postgres",
            "sh", "-lc",
            'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At'
        ]

        result = subprocess.run(
            command,
            cwd=ROOT,
            input=sql,
            text=True,
            capture_output=True,
            timeout=15,
        )

        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "Queue occupancy query failed")

        values = result.stdout.strip().split("|")
        if len(values) != 2:
            raise RuntimeError(f"Unexpected queue occupancy result: {result.stdout!r}")

        sample = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "global_live_occupancy": int(values[0]),
            "test_run_live_occupancy": int(values[1]),
        }

        with self.lock:
            self.samples.append(sample)

    def _run(self):
        while not self.stop_event.is_set():
            try:
                self._sample()
            except Exception as exc:
                with self.lock:
                    self.errors.append(str(exc))

            self.stop_event.wait(self.interval_seconds)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=20)

        # Take one final sample after the periodic sampler has stopped.
        try:
            self._sample()
        except Exception as exc:
            with self.lock:
                self.errors.append(str(exc))

        with self.lock:
            samples = list(self.samples)
            errors = list(self.errors)

        peak_global = max(
            (item["global_live_occupancy"] for item in samples),
            default=0,
        )
        peak_test = max(
            (item["test_run_live_occupancy"] for item in samples),
            default=0,
        )

        return {
            "configured_capacity": self.capacity,
            "sampling_interval_seconds": self.interval_seconds,
            "measurement_method": "periodic database samples; brief spikes may be missed",
            "sample_count": len(samples),
            "sampling_errors": errors,
            "peak_global_live_occupancy": peak_global,
            "peak_test_run_live_occupancy": peak_test,
            "peak_global_capacity_utilization_percent": round(
                peak_global / self.capacity * 100, 2
            ) if self.capacity else 0,
            "peak_test_run_capacity_utilization_percent": round(
                peak_test / self.capacity * 100, 2
            ) if self.capacity else 0,
        }


def http_request(url, method="GET", headers=None, body=None, timeout=30):
    request = urllib.request.Request(
        url,
        data=body,
        headers=headers or {},
        method=method
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            return response.status, raw

    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        return exc.code, raw


def generate_recipients(total, run_id, seed):
    rng = random.Random(seed)
    template_names = list(TEMPLATES)
    recipients = []

    for i in range(1, total + 1):
        template_id = rng.choice(template_names)

        recipients.append({
            "template_id": template_id,
            "to_email": f"loadtest-{run_id}-{i:06d}@example.com",
            "to_name": f"Load Test User {i}",
            "variables": TEMPLATES[template_id](i)
        })

    return recipients


def split_batches(recipients, number_of_batches):
    count = len(recipients)
    batch_count = min(number_of_batches, count)

    quotient, remainder = divmod(count, batch_count)
    batches = []
    start = 0

    for i in range(batch_count):
        size = quotient + (1 if i < remainder else 0)
        batches.append(recipients[start:start + size])
        start += size

    return batches


def submit_batch(
    batch_number,
    recipients,
    api_key,
    run_id,
    timeout,
    max_retries=5,
    backoff_base=0.5,
    backoff_max=10,
):
    pending = list(recipients)
    outcomes = {}
    attempt_logs = []
    total_latency = 0.0
    last_status = 0
    last_batch_id = None

    for partial_round in range(max_retries + 1):
        if not pending:
            break

        # A partial 202 requires a new key because the original
        # successful response is stored by the API.
        idem_key = (
            f"loadtest-{run_id}-batch-{batch_number}"
            if partial_round == 0
            else f"loadtest-{run_id}-batch-{batch_number}-partial-{partial_round}"
        )

        payload = json.dumps({"recipients": pending}).encode("utf-8")

        headers = {
            "X-API-Key": api_key,
            "Content-Type": "application/json",
            "Idempotency-Key": idem_key,
        }

        response_data = {}
        response_text = ""
        status = 0

        # Retry the exact same request/key for 503, 409 and
        # ambiguous transport failures.
        for transport_attempt in range(max_retries + 1):
            started = time.perf_counter()
            error = None

            try:
                status, response_text = http_request(
                    f"{API_URL}/batches",
                    method="POST",
                    headers=headers,
                    body=payload,
                    timeout=timeout,
                )

                try:
                    response_data = json.loads(response_text)
                except json.JSONDecodeError:
                    response_data = {}

            except Exception as exc:
                status = 0
                error = type(exc).__name__
                response_data = {}

            elapsed = time.perf_counter() - started
            total_latency += elapsed
            last_status = status

            # Preserve the API response details for retry diagnostics.
            response_error = (
                response_data.get("detail")
                or response_data.get("error")
                or response_data.get("message")
                or ""
            )

            if status == 503:
                retry_reason = "http_503_service_unavailable"
            elif status == 409:
                retry_reason = "http_409_idempotency_conflict"
            elif status == 0:
                retry_reason = f"transport_error_{error or 'unknown'}"
            else:
                retry_reason = ""

            attempt_logs.append({
                "batch_number": batch_number,
                "partial_round": partial_round,
                "transport_attempt": transport_attempt + 1,
                "http_status": status,
                "latency_seconds": round(elapsed, 4),
                "error_type": error,
                "retry_reason": retry_reason,
                "response_error": str(response_error)[:500],
                "response_body_preview": response_text[:500],
                "server_busy_recipients": (
                    sum(
                        item.get("reason") == "server_busy"
                        for item in response_data.get("results", [])
                    ) if status == 202 else 0
                ),
                "queue_capacity_exhausted": (
                    status == 503
                    and "queue_capacity_exhausted" in response_text
                ),
                "idempotency_key": idem_key,
            })

            if status in (503, 409, 0):
                if transport_attempt < max_retries:
                    delay = min(
                        backoff_base * (2 ** transport_attempt),
                        backoff_max,
                    )
                    time.sleep(delay)
                    continue

            break

        last_batch_id = response_data.get("batch_id")

        if status == 202:
            result_by_email = {
                item.get("to_email"): item
                for item in response_data.get("results", [])
                if item.get("to_email")
            }

            busy_recipients = []

            for recipient in pending:
                email = recipient["to_email"]
                item = result_by_email.get(email)

                if item is None:
                    outcomes[email] = {
                        "status": "unsubmitted",
                        "reason": "missing_api_result",
                        "attempts": partial_round + 1,
                    }
                    continue

                if item.get("status") == "accepted":
                    outcomes[email] = {
                        "status": "accepted",
                        "reason": "",
                        "attempts": partial_round + 1,
                    }

                elif item.get("reason") == "server_busy":
                    busy_recipients.append(recipient)

                else:
                    outcomes[email] = {
                        "status": "rejected",
                        "reason": item.get("reason", "permanent_rejection"),
                        "attempts": partial_round + 1,
                    }

            pending = busy_recipients

            if not pending:
                break

            if partial_round < max_retries:
                delay = min(
                    backoff_base * (2 ** partial_round),
                    backoff_max,
                )
                time.sleep(delay)
                continue

            break

        elif status == 503:
            # No successful batch/idempotency record is stored
            # for a zero-accepted response.
            break

        elif status == 422:
            response_results = {
                item.get("to_email"): item
                for item in response_data.get("results", [])
                if item.get("to_email")
            }

            for recipient in pending:
                item = response_results.get(recipient["to_email"], {})
                outcomes[recipient["to_email"]] = {
                    "status": "rejected",
                    "reason": item.get(
                        "reason",
                        response_data.get("error", "validation_error"),
                    ),
                    "attempts": len(attempt_logs),
                }

            pending = []
            break

        else:
            # 409 or network failure after retries, or another
            # unexpected HTTP response.
            break

    # Anything still pending was not successfully submitted.
    for recipient in pending:
        email = recipient["to_email"]

        if email not in outcomes:
            outcomes[email] = {
                "status": "unsubmitted",
                "reason": (
                    "queue_capacity_or_retry_limit"
                    if last_status == 503
                    else f"request_failed_http_{last_status}"
                ),
                "attempts": len(attempt_logs),
            }

    return {
        "batch_number": batch_number,
        "requested": len(recipients),
        "http_status": last_status,
        "latency_seconds": round(total_latency, 4),
        "accepted": sum(
            item["status"] == "accepted"
            for item in outcomes.values()
        ),
        "rejected": sum(
            item["status"] == "rejected"
            for item in outcomes.values()
        ),
        "unsubmitted": sum(
            item["status"] == "unsubmitted"
            for item in outcomes.values()
        ),
        "retry_attempts": max(0, len(attempt_logs) - 1),
        "batch_id": last_batch_id,
        "attempt_logs": attempt_logs,
        "recipient_outcomes": outcomes,
    }



def query_database(run_id):
    """Read only the database rows belonging to this test run."""
    sql = f"""
        WITH latest AS (
            SELECT DISTINCT ON (to_email)
                to_email,
                status
            FROM emails
            WHERE to_email LIKE 'loadtest-{run_id}-%@example.com'
            ORDER BY to_email, created_at DESC, id DESC
        )
        SELECT status, COUNT(*)
        FROM latest
        GROUP BY status
        ORDER BY status;
    """

    command = [
        "docker", "compose", "exec", "-T", "postgres",
        "sh", "-lc",
        'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -F "|"'
    ]

    result = subprocess.run(
        command,
        cwd=ROOT,
        input=sql,
        text=True,
        capture_output=True,
        timeout=20
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Database query failed: " + result.stderr.strip()
        )

    counts = {}

    for line in result.stdout.splitlines():
        if not line.strip():
            continue

        status, count = line.split("|", 1)
        counts[status] = int(count)

    return counts


def wait_for_completion(run_id, expected, timeout, poll_interval):
    started = time.perf_counter()
    last_counts = {}

    terminal = {"sent", "failed", "rejected"}

    while time.perf_counter() - started < timeout:
        last_counts = query_database(run_id)
        total = sum(last_counts.values())

        print(
            f"\rDatabase: {total}/{expected} records | "
            f"sent={last_counts.get('sent', 0)} "
            f"failed={last_counts.get('failed', 0)} "
            f"accepted={last_counts.get('accepted', 0)} "
            f"queued={last_counts.get('queued', 0)} "
            f"retrying={last_counts.get('retrying', 0)}",
            end="",
            flush=True
        )

        if total >= expected:
            non_terminal = sum(
                count for status, count in last_counts.items()
                if status not in terminal
            )

            if non_terminal == 0:
                print()
                return last_counts, True

        time.sleep(poll_interval)

    print()
    return last_counts, False


def verify_mailpit(run_id, expected, page_size=500):
    """Match only this run's recipients using efficient pattern matching."""
    matched = set()
    total_messages = 0
    start = 0

    pattern = re.compile(
        rf"loadtest-{re.escape(run_id)}-\d{{6}}@example\.com"
    )

    while True:
        url = (
            f"{MAILPIT_URL}/api/v1/messages"
            f"?start={start}&limit={page_size}"
        )

        status, raw = http_request(url, timeout=30)

        if status != 200:
            raise RuntimeError(
                f"Mailpit returned HTTP {status}: {raw[:500]}"
            )

        data = json.loads(raw)
        messages = data.get("messages", [])
        total_messages = data.get("total", total_messages)

        for message in messages:
            serialized = json.dumps(message)
            matched.update(pattern.findall(serialized))

        start += len(messages)

        if not messages or start >= total_messages:
            break

    return {
        "mailpit_total_messages": total_messages,
        "matching_test_messages": len(matched),
        "missing_messages": max(0, expected - len(matched))
    }


def percentile(values, percentile_value):
    if not values:
        return 0

    ordered = sorted(values)
    index = round((len(ordered) - 1) * percentile_value / 100)
    return ordered[index]


def write_reports(output_dir, summary, batch_results, recipients, run_id, recipient_outcomes=None):
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = output_dir / f"performance-{run_id}.json"
    batch_path = output_dir / f"batches-{run_id}.csv"
    recipients_path = output_dir / f"recipients-{run_id}.csv"

    json_path.write_text(json.dumps(summary, indent=2))

    with batch_path.open("w", newline="") as file:
        fields = [
            "batch_number", "requested", "http_status",
            "latency_seconds", "accepted", "rejected",
            "unsubmitted", "retry_attempts", "batch_id",
            "attempt_statuses", "retry_reasons",
            "attempt_error_types", "attempt_response_errors"
        ]

        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()

        for result in batch_results:
            attempts = result.get("attempt_logs", [])

            writer.writerow({
                **{
                    key: result.get(key, "")
                    for key in fields
                },
                "attempt_statuses": ";".join(
                    str(item.get("http_status", ""))
                    for item in attempts
                ),
                "retry_reasons": ";".join(
                    item.get("retry_reason", "")
                    for item in attempts
                    if item.get("retry_reason")
                ),
                "attempt_error_types": ";".join(
                    item.get("error_type") or ""
                    for item in attempts
                ),
                "attempt_response_errors": ";".join(
                    item.get("response_error") or ""
                    for item in attempts
                ),
            })

    with recipients_path.open("w", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "template_id", "to_email",
                "final_status", "reason", "attempts"
            ]
        )
        writer.writeheader()

        for recipient in recipients:
            outcome = (recipient_outcomes or {}).get(
                recipient["to_email"], {}
            )
            writer.writerow({
                "template_id": recipient["template_id"],
                "to_email": recipient["to_email"],
                "final_status": outcome.get("status", "unknown"),
                "reason": outcome.get("reason", ""),
                "attempts": outcome.get("attempts", 0),
            })

    print("\nReports created:")
    print(json_path)
    print(batch_path)
    print(recipients_path)


def prompt_positive_integer(label):
    """Prompt until the user enters a positive integer."""
    while True:
        value = input(f"{label}: ").strip()
        try:
            number = int(value)
            if number > 0:
                return number
        except ValueError:
            pass
        print("Please enter a positive whole number.")


def get_test_inputs(args, parser):
    """Use CLI values when supplied; interactively prompt for missing values."""
    interactive = any(
        value is None
        for value in (args.total, args.batches, args.connections)
    )

    if interactive:
        if not sys.stdin.isatty():
            parser.error(
                "--total, --batches, and --connections are required "
                "when running non-interactively."
            )

        print("\\nEnter performance test configuration:")
        if args.total is None:
            args.total = prompt_positive_integer("Total number of emails")
        if args.batches is None:
            args.batches = prompt_positive_integer("Number of batches")
        if args.connections is None:
            args.connections = prompt_positive_integer(
                "Number of concurrent connections"
            )

    if args.total < 1 or args.batches < 1 or args.connections < 1:
        parser.error("total, batches, and connections must be positive.")

    if args.batches > args.total:
        parser.error("batches cannot exceed total emails.")

    return interactive


def main():
    global API_URL, MAILPIT_URL

    parser = argparse.ArgumentParser(
        description="Email API performance and delivery tester"
    )

    parser.add_argument("--total", type=int, default=None)
    parser.add_argument("--batches", type=int, default=None)
    parser.add_argument("--connections", type=int, default=None)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--request-timeout", type=int, default=60)
    parser.add_argument("--poll-interval", type=float, default=2)
    parser.add_argument("--api-url", default=API_URL)
    parser.add_argument("--mailpit-url", default=MAILPIT_URL)
    parser.add_argument("--skip-delivery-check", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--backoff-base", type=float, default=0.5)
    parser.add_argument("--backoff-max", type=float, default=10.0)

    args = parser.parse_args()
    interactive = get_test_inputs(args, parser)

    API_URL = args.api_url.rstrip("/")
    MAILPIT_URL = args.mailpit_url.rstrip("/")

    os.chdir(ROOT)

    env = load_env()
    api_key = env.get("EMAIL_API_KEY", "")

    if not api_key:
        raise RuntimeError("EMAIL_API_KEY is missing from .env.")

    random.seed(args.seed)
    run_id = uuid.uuid4().hex[:12]
    recipients = generate_recipients(args.total, run_id, args.seed)
    batches = split_batches(recipients, args.batches)

    template_counts = {}

    for recipient in recipients:
        name = recipient["template_id"]
        template_counts[name] = template_counts.get(name, 0) + 1

    print("=" * 65)
    print("EMAIL API PERFORMANCE TEST")
    print("=" * 65)
    print(f"Run ID: {run_id}")
    print(f"Total emails: {args.total}")
    print(f"Batches: {len(batches)}")
    print(f"Concurrent API requests: {args.connections}")
    print(f"Average batch size: {args.total / len(batches):.1f}")
    print(f"Template distribution: {template_counts}")
    print("API key: loaded from .env (hidden)")
    print("=" * 65)

    if interactive and not args.dry_run:
        while True:
            confirmation = input("Start performance test? (y/n): ").strip().lower()
            if confirmation in ("y", "yes"):
                break
            if confirmation in ("n", "no"):
                print("Performance test cancelled. No emails were submitted.")
                return
            print("Please enter y or n.")

    if args.dry_run:
        print("\nDRY RUN: No emails will be submitted.")
        print("Batch distribution:")

        for index, batch in enumerate(batches, start=1):
            print(f"  Batch {index}: {len(batch)} recipients")

        print(f"Total generated recipients: {len(recipients)}")
        print(f"Template distribution: {template_counts}")
        print("No API requests or database changes were made.")
        return

    # Verify connectivity before submitting any emails.
    status, _ = http_request(f"{API_URL}/health", timeout=10)

    if status != 200:
        raise RuntimeError(f"API health check failed: HTTP {status}")

    mailpit_status, _ = http_request(
        f"{MAILPIT_URL}/api/v1/messages?start=0&limit=1",
        timeout=10
    )

    if mailpit_status != 200:
        raise RuntimeError("Mailpit health/API check failed.")

    queue_capacity = int(env.get("QUEUE_CAPACITY", "1000"))
    queue_monitor = QueueOccupancyMonitor(
        run_id=run_id,
        capacity=queue_capacity,
        interval_seconds=0.5,
    )
    queue_monitor.start()

    started_at_utc = datetime.now(timezone.utc).isoformat()
    test_started = time.perf_counter()
    submission_started = time.perf_counter()
    batch_results = []

    print("\nSubmitting batches...")

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.connections
    ) as executor:

        futures = [
            executor.submit(
                submit_batch,
                index + 1,
                batch,
                api_key,
                run_id,
                args.request_timeout,
                args.max_retries,
                args.backoff_base,
                args.backoff_max,
            )
            for index, batch in enumerate(batches)
        ]

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            batch_results.append(result)

            print(
                f"Batch {result['batch_number']}: "
                f"HTTP {result['http_status']} | "
                f"accepted={result['accepted']} | "
                f"rejected={result['rejected']} | "
                f"latency={result['latency_seconds']:.3f}s"
            )

    submission_time = time.perf_counter() - submission_started

    batch_results.sort(key=lambda item: item["batch_number"])

    recipient_outcomes = {}

    for batch_result in batch_results:
        recipient_outcomes.update(
            batch_result.get("recipient_outcomes", {})
        )

    accepted = sum(
        item["status"] == "accepted"
        for item in recipient_outcomes.values()
    )
    rejected = sum(
        item["status"] == "rejected"
        for item in recipient_outcomes.values()
    )
    unsubmitted = sum(
        item["status"] == "unsubmitted"
        for item in recipient_outcomes.values()
    )

    all_attempts = [
        attempt
        for batch_result in batch_results
        for attempt in batch_result.get("attempt_logs", [])
    ]

    successful_requests = sum(
        attempt["http_status"] == 202
        for attempt in all_attempts
    )

    latencies = [
        attempt["latency_seconds"]
        for attempt in all_attempts
    ]

    total_retry_attempts = sum(
        item.get("retry_attempts", 0)
        for item in batch_results
    )

    print("\nWaiting for database delivery status...")

    completion_started = time.perf_counter()

    db_counts, completed = wait_for_completion(
        run_id,
        accepted,
        args.timeout,
        args.poll_interval
    )

    completion_time = time.perf_counter() - completion_started
    queue_occupancy = queue_monitor.stop()

    capacity_503_attempts = sum(
        bool(item.get("queue_capacity_exhausted"))
        for item in all_attempts
    )
    server_busy_recipient_events = sum(
        item.get("server_busy_recipients", 0)
        for item in all_attempts
    )

    queue_occupancy["capacity_503_attempts"] = capacity_503_attempts
    queue_occupancy["server_busy_recipient_events"] = server_busy_recipient_events

    pipeline_end_to_end_time = time.perf_counter() - test_started

    delivery = None
    mailpit_verification_time = 0.0

    if not args.skip_delivery_check and completed and accepted > 0:
        print("Verifying matching messages in Mailpit...")
        verification_started = time.perf_counter()
        delivery = verify_mailpit(run_id, accepted)
        mailpit_verification_time = time.perf_counter() - verification_started

    full_test_time = time.perf_counter() - test_started

    summary = {
        "run_id": run_id,
        "started_at_utc": started_at_utc,
        "configuration": {
            "total_requested": args.total,
            "number_of_batches": len(batches),
            "concurrent_api_requests": args.connections,
            "seed": args.seed
        },
        "templates": template_counts,
        "submission": {
            "submission_time_seconds": round(submission_time, 3),
            "successful_http_202_requests": successful_requests,
            "accepted": accepted,
            "rejected": rejected,
            "unsubmitted": unsubmitted,
            "retry_attempts": total_retry_attempts,
            "total_http_attempts": len(all_attempts),
            "request_latency_average_seconds": (
                statistics.mean(latencies) if latencies else 0
            ),
            "request_latency_p50_seconds": percentile(latencies, 50),
            "request_latency_p95_seconds": percentile(latencies, 95),
            "request_latency_p99_seconds": percentile(latencies, 99),
            "accepted_emails_per_second": (
                accepted / submission_time if submission_time else 0
            )
        },
        "performance": {
            "api_submission_throughput_emails_per_second": round(
                accepted / submission_time if submission_time else 0, 2
            ),
            "end_to_end_throughput_emails_per_second": round(
                accepted / pipeline_end_to_end_time
                if pipeline_end_to_end_time else 0, 2
            ),
            "pipeline_end_to_end_seconds": round(
                pipeline_end_to_end_time, 3
            ),
            "database_completion_wait_seconds": round(completion_time, 3),
            "mailpit_verification_seconds": round(
                mailpit_verification_time, 3
            ),
            "full_test_duration_seconds": round(full_test_time, 3)
        },
        "queue_occupancy": queue_occupancy,
        "database": {
            "status_counts": db_counts,
            "completed_before_timeout": completed,
            "completion_wait_seconds": round(completion_time, 3)
        },
        "mailpit": delivery
    }

    write_reports(
        ROOT / "tests" / "load" / "reports",
        summary,
        batch_results,
        recipients,
        run_id,
        recipient_outcomes,
    )

    performance = summary["performance"]

    print("\n" + "=" * 60)
    print("           EMAIL PERFORMANCE RESULTS")
    print("=" * 60)
    print(f"Run ID:                         {run_id}")
    print(f"Emails requested:               {args.total}")
    print(f"Emails accepted:                {accepted}")
    print(f"Emails rejected:                {rejected}")
    print(f"Emails unsubmitted:             {unsubmitted}")
    print(f"API batches:                    {len(batches)}")
    print(f"Concurrent API requests:        {args.connections}")
    print("-" * 60)
    print("THROUGHPUT")
    print(f"API submission throughput:      {performance['api_submission_throughput_emails_per_second']:.2f} emails/sec")
    print(f"END-TO-END throughput:           {performance['end_to_end_throughput_emails_per_second']:.2f} emails/sec")
    print("-" * 60)
    print("TIMING")
    print(f"API submission time:            {submission_time:.3f} sec")
    print(f"Database completion wait:       {completion_time:.3f} sec")
    print(f"End-to-end pipeline time:       {pipeline_end_to_end_time:.3f} sec")
    print(f"Mailpit verification time:      {mailpit_verification_time:.3f} sec")
    print(f"Full test duration:             {full_test_time:.3f} sec")
    print("-" * 60)
    print("API LATENCY")
    print(f"Average:                        {statistics.mean(latencies) if latencies else 0:.3f} sec")
    print(f"P50:                            {percentile(latencies, 50):.3f} sec")
    print(f"P95:                            {percentile(latencies, 95):.3f} sec")
    print(f"P99:                            {percentile(latencies, 99):.3f} sec")
    print("-" * 60)
    print("QUEUE CAPACITY")
    print(f"Configured capacity:             {queue_occupancy['configured_capacity']}")
    print(f"Peak global live occupancy:      {queue_occupancy['peak_global_live_occupancy']}")
    print(f"Peak test-run live occupancy:    {queue_occupancy['peak_test_run_live_occupancy']}")
    print(f"Peak global utilization:         {queue_occupancy['peak_global_capacity_utilization_percent']:.2f}%")
    print(f"Peak test-run utilization:       {queue_occupancy['peak_test_run_capacity_utilization_percent']:.2f}%")
    print(f"Capacity-exhausted HTTP 503s:    {capacity_503_attempts}")
    print(f"Server-busy recipient events:    {server_busy_recipient_events}")
    print(f"Occupancy samples:               {queue_occupancy['sample_count']}")
    if queue_occupancy["sampling_errors"]:
        print(f"Monitoring errors:               {len(queue_occupancy['sampling_errors'])}")
    print("-" * 60)
    print("DELIVERY")
    print(f"Database status counts:         {db_counts}")
    print(f"Retry attempts:                 {total_retry_attempts}")
    if delivery is not None:
        print(f"Mailpit total messages:         {delivery['mailpit_total_messages']}")
        print(f"Matching test messages:         {delivery['matching_test_messages']}")
        print(f"Missing messages:               {delivery['missing_messages']}")
    else:
        print("Mailpit verification:           Not performed")
    print("=" * 60)

    print("\n===== TEST SUMMARY =====")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nTest interrupted. No existing records or messages were deleted.")
        sys.exit(130)
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
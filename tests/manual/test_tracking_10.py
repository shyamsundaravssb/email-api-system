import json
import os
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = ROOT / ".env"

ACCOUNT_ID = "f56acb23-c62a-45bc-b1a5-d73f13b853bd"
BATCH_SIZE = 10
API_URL = "http://127.0.0.1:8001/internal/generate"
DB_CONTAINER = "email-api-system-postgres-1"
RABBIT_QUEUE = "email.send"


def read_env():
    values = {}
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip("'\"")
    return values


def run_command(command, timeout=30):
    result = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            result.stderr.strip() or result.stdout.strip()
        )
    return result.stdout.strip()


def psql(query):
    return run_command([
        "sudo", "-n", "docker", "exec",
        DB_CONTAINER,
        "psql", "-U", "emailapi", "-d", "emailapi",
        "-X", "-A", "-t", "-F", "|",
        "-v", "ON_ERROR_STOP=1",
        "-c", query,
    ])


def get_internal_token():
    values = read_env()
    token = os.getenv("INTERNAL_TOKEN") or values.get("INTERNAL_TOKEN")

    if token:
        return token

    token = run_command([
        "sudo", "-n", "docker", "compose",
        "exec", "-T", "generation-api",
        "printenv", "INTERNAL_TOKEN",
    ])

    if not token:
        raise RuntimeError("INTERNAL_TOKEN is empty.")

    return token


def sql_quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def check_prerequisites():
    print("\n[1/5] Checking password_reset template...")

    template = psql("""
        SELECT template_id
        FROM templates
        WHERE template_id = 'password_reset'
          AND required_variables @> ARRAY['name', 'link']::text[];
    """)

    if template != "password_reset":
        raise RuntimeError(
            "password_reset template is missing or does not contain "
            "the required variables: name and link."
        )

    print("      Template and required variables are correct.")

    print("\n[2/5] Checking sender account...")

    account = psql(f"""
        SELECT id, from_email, COALESCE(from_name, '')
        FROM accounts
        WHERE id = '{ACCOUNT_ID}';
    """)

    if not account:
        raise RuntimeError("Configured sender account was not found.")

    fields = account.split("|", 2)

    if len(fields) != 3 or fields[0] != ACCOUNT_ID or not fields[1]:
        raise RuntimeError(f"Unexpected sender account result: {account}")

    print("      Sender:", fields[1])

    print("\n[3/5] Checking Generation API token...")
    token = get_internal_token()

    if not token:
        raise RuntimeError("Generation API token is unavailable.")

    print("      Internal token is available.")

    return {
        "from_email": fields[1],
        "from_name": fields[2] or "Grasshopper",
        "token": token,
    }


def create_batch():
    print("\n[4/5] Creating 10 unique test email records...")

    batch_id = str(uuid.uuid4())
    inputs = []

    for position in range(1, BATCH_SIZE + 1):
        recipient = f"tracking-{secrets.token_hex(12)}@example.com"
        recipient_name = f"Tracking Test {position}"
        original_link = (
            "https://example.com/reset-password?test_id="
            + str(uuid.uuid4())
        )

        inputs.append(
            f"({position}, "
            f"{sql_quote(recipient)}, "
            f"{sql_quote(recipient_name)}, "
            f"{sql_quote(original_link)})"
        )

    values_sql = ",\n".join(inputs)

    sql = f"""
    WITH new_batch AS (
        INSERT INTO batches (
            id,
            account_id,
            total_count,
            accepted_count,
            rejected_count
        )
        VALUES (
            '{batch_id}'::uuid,
            '{ACCOUNT_ID}'::uuid,
            {BATCH_SIZE},
            {BATCH_SIZE},
            0
        )
        RETURNING id
    ),
    input_rows(position, to_email, to_name, original_link) AS (
        VALUES
        {values_sql}
    ),
    inserted AS (
        INSERT INTO emails (
            batch_id,
            position,
            template_id,
            to_email,
            to_name,
            variables,
            status
        )
        SELECT
            b.id,
            i.position,
            'password_reset',
            i.to_email,
            i.to_name,
            jsonb_build_object(
                'name', i.to_name,
                'link', i.original_link
            ),
            'accepted'
        FROM new_batch b
        CROSS JOIN input_rows i
        RETURNING
            id,
            batch_id,
            tracking_token,
            position,
            template_id,
            to_email,
            to_name,
            variables
    )
    SELECT
        id,
        batch_id,
        tracking_token,
        position,
        template_id,
        to_email,
        to_name,
        variables::text
    FROM inserted
    ORDER BY position;
    """

    output = psql(sql)
    records = []

    for line in output.splitlines():
        fields = line.split("|", 7)

        if len(fields) != 8:
            continue

        (
            email_id,
            returned_batch_id,
            tracking_token,
            position,
            template_id,
            to_email,
            to_name,
            variables,
        ) = fields

        if returned_batch_id != batch_id:
            raise RuntimeError("Unexpected batch ID returned by database.")

        records.append({
            "email_id": email_id,
            "tracking_token": tracking_token,
            "template_id": template_id,
            "to_email": to_email,
            "to_name": to_name,
            "variables": json.loads(variables),
        })

    if len(records) != BATCH_SIZE:
        raise RuntimeError(
            f"Expected 10 email rows but database returned {len(records)}."
        )

    if len({r["to_email"] for r in records}) != BATCH_SIZE:
        raise RuntimeError("Duplicate recipient addresses detected.")

    if len({r["tracking_token"] for r in records}) != BATCH_SIZE:
        raise RuntimeError("Duplicate tracking tokens detected.")

    print("      Batch ID:", batch_id)
    print("      Inserted emails:", len(records))
    print("      Unique tracking tokens:", len({
        r["tracking_token"] for r in records
    }))

    return batch_id, records


def submit_batch(batch_id, records, sender):
    print("\n[5/5] Submitting batch to Generation API...")

    body = {
        "batch_id": batch_id,
        "from_email": sender["from_email"],
        "from_name": sender["from_name"],
        "items": records,
    }

    request = urllib.request.Request(
        API_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Internal-Token": sender["token"],
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            result = json.loads(response.read().decode("utf-8"))

            print("      HTTP status:", response.status)
            print("      API response:", result)

            if response.status != 202:
                raise RuntimeError("Expected HTTP 202.")

            if result.get("batch_id") != batch_id:
                raise RuntimeError("API returned an unexpected batch ID.")

            if result.get("received") != BATCH_SIZE:
                raise RuntimeError("API did not receive all 10 emails.")

    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Generation API returned HTTP {error.code}: {detail}"
        )

    return batch_id


def wait_for_processing(batch_id):
    print("\nWaiting for Generation API processing...")

    statuses = {}

    for attempt in range(45):
        output = psql(f"""
            SELECT status, count(*)
            FROM emails
            WHERE batch_id = '{batch_id}'
            GROUP BY status
            ORDER BY status;
        """)

        statuses = {}
        for line in output.splitlines():
            if "|" in line:
                status, count = line.split("|", 1)
                statuses[status] = int(count)

        queued = statuses.get("queued", 0)
        failed = statuses.get("failed", 0)
        completed = queued + failed

        print(
            f"  Progress: queued={queued}, failed={failed}, "
            f"processed={completed}/10",
            flush=True,
        )

        if completed == BATCH_SIZE:
            break

        time.sleep(1)

    print("\nFinal statuses:", statuses)
    return statuses


def main():
    print("=" * 55)
    print(" GENERATION API - 10 EMAIL TRACKING TEST")
    print("=" * 55)

    print("\nAuthenticating sudo...")
    run_command(["sudo", "-v"], timeout=60)
    print("      Sudo authentication successful.")

    sender = check_prerequisites()

    batch_id, records = create_batch()

    print("\nSubmitting 10 emails...")
    submit_batch(batch_id, records, sender)

    statuses = wait_for_processing(batch_id)

    print("\n" + "=" * 55)
    print(" FINAL RESULTS")
    print("=" * 55)
    print("Batch ID:", batch_id)
    print("Queued:", statuses.get("queued", 0))
    print("Failed:", statuses.get("failed", 0))

    print("\nTracking mapping count:")
    mapping_count = psql(f"""
        SELECT count(*)
        FROM tracking_links tl
        JOIN emails e ON e.id = tl.email_id
        WHERE e.batch_id = '{batch_id}';
    """)
    print("Click mappings:", mapping_count)

    pixel_count = psql(f"""
        SELECT count(*)
        FROM emails
        WHERE batch_id = '{batch_id}'
          AND tracking_token IS NOT NULL;
    """)
    print("Open tracking tokens:", pixel_count)

    print("\nBatch ID for further inspection:", batch_id)

    if statuses.get("queued", 0) == BATCH_SIZE:
        print("\nSUCCESS: All 10 emails reached queued status.")
    else:
        print("\nCHECK REQUIRED: Not all emails reached queued status.")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("\nTEST FAILED:", error, file=sys.stderr, flush=True)
        sys.exit(1)

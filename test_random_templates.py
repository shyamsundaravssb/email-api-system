import json
import time
import random
import secrets
import uuid
import urllib.request
import urllib.error
from collections import Counter

API_URL = "http://127.0.0.1:8003/batches"
MAILPIT_URL = "http://127.0.0.1:8026/api/v1/messages?start=0&limit=1000"
API_KEY = "dev-api-key"
TOTAL = 1000

TEMPLATE_FIELDS = {
    "welcome_email": {"name"},
    "otp_email": {"name", "otp"},
    "password_reset": {"name", "link"},
}

def get_json(url, method="GET", data=None, headers=None):
    req = urllib.request.Request(
        url, data=data, headers=headers or {}, method=method
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode())

# Balanced random distribution.
template_ids = list(TEMPLATE_FIELDS) * (TOTAL // 3)
template_ids += random.choices(list(TEMPLATE_FIELDS), k=TOTAL % 3)
random.shuffle(template_ids)

prefix = f"mixed-{int(time.time())}"
recipients = []

for i, template_id in enumerate(template_ids, 1):
    name = f"Test User {i:04d}"

    if template_id == "welcome_email":
        variables = {"name": name}

    elif template_id == "otp_email":
        variables = {
            "name": name,
            "otp": f"{secrets.randbelow(1000000):06d}"
        }

    else:
        variables = {
            "name": name,
            "link": f"https://example.com/reset/{secrets.token_urlsafe(24)}"
        }

    recipients.append({
        "template_id": template_id,
        "to_email": f"{prefix}-{i:04d}@example.com",
        "to_name": name,
        "variables": variables
    })

# Validate required variables and template IDs.
errors = []

for i, r in enumerate(recipients, 1):
    required = TEMPLATE_FIELDS.get(r["template_id"])
    if required is None:
        errors.append(f"Recipient {i}: unknown template")
        continue

    missing = required - set(r["variables"])
    empty = {
        key for key in required
        if not isinstance(r["variables"].get(key), str)
        or not r["variables"][key].strip()
    }

    if missing or empty:
        errors.append(
            f"Recipient {i}: missing={missing}, empty={empty}"
        )

if errors:
    print("Payload validation failed:", errors[:20])
    raise SystemExit(1)

payload = {"recipients": recipients}
expected = {r["to_email"].lower() for r in recipients}
template_counts = Counter(template_ids)

print("========== PAYLOAD VALIDATION ==========")
print("Total:", TOTAL)
print("Templates:", dict(template_counts))
print("Missing variables: 0")
print("Password reset variables: name, link")
print("Payload validation: PASS")

# Start timing immediately before submission.
start = time.perf_counter()

try:
    result = get_json(
        API_URL,
        method="POST",
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "X-API-Key": API_KEY,
            "Idempotency-Key": str(uuid.uuid4())
        }
    )
except urllib.error.HTTPError as e:
    print("HTTP error:", e.code)
    print(e.read().decode())
    raise SystemExit(1)

print("\n========== INGESTION RESULT ==========")
print("Batch ID:", result.get("batch_id"))
print("Accepted:", result.get("accepted"))
print("Rejected:", result.get("rejected"))
print("Error:", result.get("error"))

results = result.get("results", [])
print("Statuses:", dict(Counter(r.get("status") for r in results)))

rejected = [r for r in results if r.get("status") == "rejected"]

# Show rejection reasons grouped by template.
rejected_by_template = Counter()
email_to_template = {
    r["to_email"].lower(): r["template_id"] for r in recipients
}

for r in rejected:
    template = email_to_template.get(r.get("to_email", "").lower(), "unknown")
    rejected_by_template[(template, r.get("reason", "unknown"))] += 1

print("Rejections by template/reason:", dict(rejected_by_template))

if rejected:
    print("\nFirst 10 rejected recipients:")
    for r in rejected[:10]:
        print(json.dumps(r))

# Only accepted recipients are eligible for delivery measurement.
accepted_addresses = {
    r["to_email"].lower()
    for r in results
    if r.get("status") == "accepted"
}

delivered = set()

# Wait for accepted messages, up to five minutes.
while time.perf_counter() - start < 300:
    try:
        data = get_json(MAILPIT_URL)
    except Exception as e:
        print("Mailpit polling error:", e)
        time.sleep(1)
        continue

    for message in data.get("messages", []):
        for recipient in message.get("To", []):
            address = recipient.get("Address", "").lower()
            if address in accepted_addresses:
                delivered.add(address)

    if len(delivered) == len(accepted_addresses):
        break

    time.sleep(0.5)

elapsed = time.perf_counter() - start
missing = accepted_addresses - delivered

delivered_by_template = Counter(
    email_to_template[address] for address in delivered
)

print("\n========== END-TO-END THROUGHPUT ==========")
print("Submitted:", TOTAL)
print("Accepted:", len(accepted_addresses))
print("Rejected:", len(rejected))
print("Delivered to Mailpit:", len(delivered))
print("Accepted but missing:", len(missing))
print(f"Elapsed time: {elapsed:.3f} seconds")
print(f"Throughput: {len(delivered) / elapsed:.2f} emails/second")

print("\nTemplate-wise delivery:")
for template in TEMPLATE_FIELDS:
    print(
        f"{template}: "
        f"{delivered_by_template[template]}/{template_counts[template]}"
    )

if missing:
    print("\nFirst 10 missing recipients:")
    print(*sorted(missing)[:10], sep="\n")

print(
    "\nRESULT:",
    "PASS" if not rejected and not missing and len(delivered) == TOTAL
    else "INCOMPLETE"
)

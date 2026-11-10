import json
import time
import urllib.request

API_URL = "http://127.0.0.1:8003/batches"
MAILPIT_URL = "http://127.0.0.1:8026/api/v1/messages?start=0&limit=1000"
API_KEY = "dev-api-key"

with open("/tmp/email-api-1000b.json") as f:
    payload = json.load(f)

# Give this test a unique recipient prefix.
prefix = f"baseline-{int(time.time())}"

for i, recipient in enumerate(payload["recipients"], 1):
    recipient["to_email"] = f"{prefix}-{i:04d}@example.com"

expected = {
    r["to_email"].lower()
    for r in payload["recipients"]
}

def get_json(url, method="GET", data=None, headers=None):
    request = urllib.request.Request(
        url,
        data=data,
        headers=headers or {},
        method=method
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode())

print("Starting baseline test")
print("Total recipients:", len(expected))
print("Prefix:", prefix)

start = time.perf_counter()

# Submit the batch.
result = get_json(
    API_URL,
    method="POST",
    data=json.dumps(payload).encode(),
    headers={
        "Content-Type": "application/json",
        "X-API-Key": API_KEY
    }
)

print("\nIngestion response:")
print(json.dumps(result, indent=2))

if result.get("accepted", 0) != len(expected):
    print("\nWARNING: Not all recipients were accepted.")
    print("Accepted:", result.get("accepted"))
    print("Submitted:", len(expected))

delivered = set()

# Wait up to five minutes for all emails to appear in Mailpit.
while time.perf_counter() - start < 300:
    data = get_json(MAILPIT_URL)

    for message in data.get("messages", []):
        for recipient in message.get("To", []):
            address = recipient.get("Address", "").lower()
            if address in expected:
                delivered.add(address)

    if len(delivered) == len(expected):
        break

    time.sleep(0.5)

elapsed = time.perf_counter() - start
missing = expected - delivered

print("\n========== BASELINE THROUGHPUT ==========")
print("Submitted:", len(expected))
print("Delivered:", len(delivered))
print("Missing:", len(missing))
print(f"Total time: {elapsed:.3f} seconds")
print(f"Throughput: {len(delivered) / elapsed:.2f} emails/second")

if missing:
    print("First missing recipients:", sorted(missing)[:10])

print("RESULT:", "PASS" if not missing else "INCOMPLETE")

import json
import random
from datetime import datetime, timezone, timedelta
from pathlib import Path

from google.cloud import storage


def load_config():
    cfg_path = Path(__file__).resolve().parents[1] / "config.json"
    with open(cfg_path, "r") as f:
        return json.load(f)


EVENT_TYPES = ["view", "add_to_cart", "remove_from_cart", "checkout_start", "purchase"]
DEVICES = ["mobile", "desktop", "tablet"]
BROWSERS = ["chrome", "safari", "edge", "firefox"]
CAMPAIGNS = ["none", "spring_sale", "email_blast", "influencer", "retargeting"]
REFERRERS = ["direct", "google", "bing", "instagram", "tiktok", "newsletter"]


def make_event(event_id: int, ingest_date: str):
    # Random timestamp within the ingest day (UTC)
    day_start = datetime.fromisoformat(ingest_date).replace(tzinfo=timezone.utc)
    ts = day_start + timedelta(seconds=random.randint(0, 86399))

    user_id = f"U{random.randint(1, 20000):05d}"
    product_num = random.randint(1, 10000)
    product_id = f"P{product_num:06d}"

    event_type = random.choices(
        EVENT_TYPES,
        weights=[70, 15, 5, 7, 3],  # realistic distribution
        k=1
    )[0]

    base = {
        "event_id": f"E{event_id:08d}",
        "user_id": user_id,
        "event_type": event_type,
        "product_id": product_id,
        "event_ts_utc": ts.isoformat(),
        "ingest_ts_utc": datetime.now(timezone.utc).isoformat(),
        # semi-structured blob
        "metadata": {}
    }

    # Common metadata fields (not always present)
    if random.random() < 0.85:
        base["metadata"]["device"] = random.choice(DEVICES)
    if random.random() < 0.70:
        base["metadata"]["browser"] = random.choice(BROWSERS)
    if random.random() < 0.55:
        base["metadata"]["referrer"] = random.choice(REFERRERS)
    if random.random() < 0.35:
        base["metadata"]["campaign"] = random.choice(CAMPAIGNS)

    # Schema drift: introduce a new field occasionally
    # Simulates product experiments / A/B tests showing up unexpectedly
    if random.random() < 0.02:
        base["metadata"]["experiment_variant"] = random.choice(["A", "B", "C"])

    # Some events naturally have extra payloads
    if event_type == "purchase":
        base["metadata"]["order_id"] = f"O{random.randint(1, 5_000_000):08d}"
        base["metadata"]["order_value_usd"] = round(random.uniform(10, 300), 2)

    return base


def upload_jsonl(bucket: str, object_path: str, rows: list[dict]):
    client = storage.Client()
    b = client.bucket(bucket)
    blob = b.blob(object_path)

    data = "\n".join(json.dumps(r) for r in rows) + "\n"
    blob.upload_from_string(data, content_type="application/json")
    print(f"Uploaded {len(rows)} records to gs://{bucket}/{object_path}")


def main():
    cfg = load_config()
    bucket = cfg["bucket"]

    ingest_date = "2026-03-11"
    object_path = f"bronze/source=events/ingest_date={ingest_date}/events.jsonl"

    events = [make_event(i, ingest_date) for i in range(1, 50_000 + 1)]
    upload_jsonl(bucket, object_path, events)


if __name__ == "__main__":
    main()

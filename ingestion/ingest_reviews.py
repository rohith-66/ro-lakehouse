import json
import random
from datetime import datetime, timezone, timedelta
from pathlib import Path

from google.cloud import storage


def load_config():
    cfg_path = Path(__file__).resolve().parents[1] / "config.json"
    with open(cfg_path, "r") as f:
        return json.load(f)


POS_PHRASES = [
    "Works exactly as described", "Great value for the price", "Exceeded expectations",
    "Quality feels premium", "Would buy again", "Fast shipping and easy to use"
]
NEG_PHRASES = [
    "Not as described", "Stopped working after a week", "Feels cheap",
    "Disappointed with the quality", "Would not recommend", "Packaging was damaged"
]
NEU_PHRASES = [
    "It’s okay for the price", "Does the job", "Average quality",
    "Nothing special but fine", "Met expectations", "Decent overall"
]


def make_review(i: int, ingest_date: str):
    day_start = datetime.fromisoformat(ingest_date).replace(tzinfo=timezone.utc)
    ts = day_start + timedelta(seconds=random.randint(0, 86399))

    product_num = random.randint(1, 10000)
    product_id = f"P{product_num:06d}"
    user_id = f"U{random.randint(1, 20000):05d}"

    # Ratings skew positive (realistic)
    rating = random.choices([1, 2, 3, 4, 5], weights=[5, 8, 15, 30, 42], k=1)[0]

    if rating >= 4:
        phrase = random.choice(POS_PHRASES)
    elif rating <= 2:
        phrase = random.choice(NEG_PHRASES)
    else:
        phrase = random.choice(NEU_PHRASES)

    # Add some variability/noise to make it feel real
    extras = [
        "", "", "",  # often no extra
        " Customer support was helpful.",
        " The instructions could be clearer.",
        " I compared it with a similar item and chose this one."
    ]

    text = phrase + random.choice(extras)

    return {
        "review_id": f"R{i:08d}",
        "user_id": user_id,
        "product_id": product_id,
        "rating": rating,
        "review_text": text,
        "review_ts_utc": ts.isoformat(),
        "ingest_ts_utc": datetime.now(timezone.utc).isoformat()
    }


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
    object_path = f"bronze/source=reviews/ingest_date={ingest_date}/reviews.jsonl"

    reviews = [make_review(i, ingest_date) for i in range(1, 20_000 + 1)]
    upload_jsonl(bucket, object_path, reviews)


if __name__ == "__main__":
    main()

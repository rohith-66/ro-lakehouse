import json
import random
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import storage


def load_config():
    cfg_path = Path(__file__).resolve().parents[1] / "config.json"
    with open(cfg_path, "r") as f:
        return json.load(f)


def make_products(n: int):
    categories = [
        "Electronics", "Home", "Kitchen", "Fitness", "Beauty",
        "Books", "Toys", "Clothing", "Outdoors", "Office"
    ]
    brands = [
        "Aster", "NorthPeak", "BlueNova", "Cedar & Co", "Vanta",
        "BrightForge", "Kairo", "Solstice", "Riverbend", "OmniGoods"
    ]

    products = []
    for i in range(1, n + 1):
        category = random.choice(categories)
        brand = random.choice(brands)

        base_price = {
            "Electronics": 79,
            "Home": 35,
            "Kitchen": 29,
            "Fitness": 45,
            "Beauty": 22,
            "Books": 14,
            "Toys": 18,
            "Clothing": 28,
            "Outdoors": 55,
            "Office": 25,
        }[category]

        price = round(random.uniform(base_price * 0.7, base_price * 2.2), 2)
        inventory = max(0, int(random.gauss(mu=120, sigma=60)))

        products.append({
            "product_id": f"P{i:06d}",
            "sku": f"{brand[:3].upper()}-{category[:3].upper()}-{i:06d}",
            "name": f"{brand} {category} Item {i}",
            "brand": brand,
            "category": category,
            "price": price,
            "currency": "USD",
            "inventory": inventory,
            "is_active": inventory > 0,
            # stable timestamp field
            "last_updated_utc": datetime.now(timezone.utc).isoformat()
        })

    return products


def upload_jsonl(bucket: str, object_path: str, rows: list[dict]):
    client = storage.Client()
    b = client.bucket(bucket)
    blob = b.blob(object_path)

    # JSON Lines format (one JSON object per line)
    data = "\n".join(json.dumps(r) for r in rows) + "\n"
    blob.upload_from_string(data, content_type="application/json")
    print(f"Uploaded {len(rows)} records to gs://{bucket}/{object_path}")


def main():
    cfg = load_config()
    bucket = cfg["bucket"]

    # Keep date explicit for reproducibility (per our project timeline)
    ingest_date = "2026-03-11"
    object_path = f"bronze/source=products/ingest_date={ingest_date}/products.jsonl"

    products = make_products(10_000)
    upload_jsonl(bucket, object_path, products)


if __name__ == "__main__":
    main()

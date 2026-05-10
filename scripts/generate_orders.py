#!/usr/bin/env python3
import json
import random
import time
from datetime import datetime, timezone


EVENT_TYPES = [
    "order-created",
    "payment-completed",
    "order-cancelled",
    "payment-failed",
]

CURRENCIES = ["TRY", "USD", "EUR", "GBP"]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def create_order_event() -> dict:
    return {
        "orderId": f"o-{random.randint(1000, 9999)}",
        "userId": f"u-{random.randint(1, 200)}",
        "amount": round(random.uniform(20, 2500), 2),
        "currency": random.choice(CURRENCIES),
        "eventType": random.choice(EVENT_TYPES),
        "eventTime": now_iso(),
    }


def main() -> None:
    while True:
        event = create_order_event()
        print(json.dumps(event), flush=True)
        time.sleep(3)


if __name__ == "__main__":
    main()

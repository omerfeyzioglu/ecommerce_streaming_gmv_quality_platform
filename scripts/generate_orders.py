#!/usr/bin/env python3
import json
import random
import time
import uuid
from copy import deepcopy
from datetime import datetime, timedelta, timezone


EVENT_TYPES = [
    "order-created",
    "payment-completed",
    "order-cancelled",
    "payment-failed",
]

CURRENCIES = ["TRY", "USD", "EUR", "GBP"]
LATE_EVENT_RATE = 0.15
DUPLICATE_EVENT_RATE = 0.10
INVALID_EVENT_RATE = 0.10
MAX_LATE_SECONDS = 30

INVALID_SCENARIOS = [
    "negative_amount",
    "missing_currency",
    "invalid_event_type",
    "malformed_json",
]

LAST_EVENT = None


def event_time_iso() -> str:
    event_time = datetime.now(timezone.utc)
    if random.random() < LATE_EVENT_RATE:
        event_time = event_time - timedelta(seconds=random.randint(5, MAX_LATE_SECONDS))

    return event_time.isoformat().replace("+00:00", "Z")


def create_order_event() -> dict:
    return {
        "eventId": str(uuid.uuid4()),
        "orderId": f"o-{random.randint(1000, 9999)}",
        "userId": f"u-{random.randint(1, 200)}",
        "amount": round(random.uniform(20, 2500), 2),
        "currency": random.choice(CURRENCIES),
        "eventType": random.choice(EVENT_TYPES),
        "eventTime": event_time_iso(),
    }


def make_invalid_event(event: dict) -> dict | str:
    scenario = random.choice(INVALID_SCENARIOS)

    if scenario == "negative_amount":
        event["amount"] = -round(random.uniform(1, 500), 2)
        return event

    if scenario == "missing_currency":
        event.pop("currency", None)
        return event

    if scenario == "invalid_event_type":
        event["eventType"] = "unknown-event-type"
        return event

    return '{"eventId": "bad-json", "amount": '


def main() -> None:
    global LAST_EVENT

    while True:
        if LAST_EVENT is not None and random.random() < DUPLICATE_EVENT_RATE:
            event = deepcopy(LAST_EVENT)
        else:
            event = create_order_event()
            LAST_EVENT = deepcopy(event)

        if random.random() < INVALID_EVENT_RATE:
            event = make_invalid_event(deepcopy(event))

        if isinstance(event, str):
            print(event, flush=True)
        else:
            print(json.dumps(event), flush=True)

        time.sleep(3)


if __name__ == "__main__":
    main()

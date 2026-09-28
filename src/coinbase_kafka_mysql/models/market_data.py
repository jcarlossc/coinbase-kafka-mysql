from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True, slots=True)
class CryptoTick:
    product_id: str
    sequence: int
    event_time: datetime

    price: Decimal

    open_24h: Decimal | None
    volume_24h: Decimal | None
    low_24h: Decimal | None
    high_24h: Decimal | None

    volume_30d: Decimal | None

    best_bid: Decimal | None
    best_ask: Decimal | None
    last_size: Decimal | None

    raw_payload: str

    @staticmethod
    def _decimal(value: Any) -> Decimal | None:
        if value is None or value == "":
            return None

        return Decimal(str(value))

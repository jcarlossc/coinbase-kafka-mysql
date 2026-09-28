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

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, Any],
        raw_payload: str,
    ) -> "CryptoTick":
        if payload.get("type") != "ticker":
            raise ValueError("O payload não é um evento de ticker.")

        product_id = str(payload.get("product_id", "")).strip()

        if not product_id:
            raise ValueError("O evento de ticker não possui product_id.")

        if payload.get("sequence") is None:
            raise ValueError("O evento de ticker não possui sequência.")

        price = cls._decimal(payload.get("price"))

        if price is None:
            raise ValueError("O evento de cotação não possui preço.")

        time_value = payload.get("time")

        if not time_value:
            raise ValueError("O evento do ticker não possui horário.")

        event_time = datetime.fromisoformat(str(time_value))

        return cls(
            product_id=product_id,
            sequence=int(payload["sequence"]),
            event_time=event_time,
            price=price,
            open_24h=cls._decimal(payload.get("open_24h")),
            volume_24h=cls._decimal(payload.get("volume_24h")),
            low_24h=cls._decimal(payload.get("low_24h")),
            high_24h=cls._decimal(payload.get("high_24h")),
            volume_30d=cls._decimal(payload.get("volume_30d")),
            best_bid=cls._decimal(payload.get("best_bid")),
            best_ask=cls._decimal(payload.get("best_ask")),
            last_size=cls._decimal(payload.get("last_size")),
            raw_payload=raw_payload,
        )

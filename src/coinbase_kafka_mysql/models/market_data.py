from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True, slots=True)
class CryptoTick:
    """Representa um evento de ticker de criptomoeda recebido da Coinbase."""

    # Identificação e metadados do evento.
    product_id: str
    sequence: int
    event_time: datetime

    # Valor de mercado exigido.
    price: Decimal

    # Estatísticas de mercado opcionais de 24 horas.
    open_24h: Decimal | None
    volume_24h: Decimal | None
    low_24h: Decimal | None
    high_24h: Decimal | None

    # Estatística de mercado de longo prazo opcional.
    volume_30d: Decimal | None

    # Informações opcionais sobre o livro de ofertas e as negociações.
    best_bid: Decimal | None
    best_ask: Decimal | None
    last_size: Decimal | None

    # Payload JSON original para rastreabilidade e auditoria.
    raw_payload: str

    @staticmethod
    def _decimal(value: Any) -> Decimal | None:
        """Converta um valor numérico para Decimal de forma segura."""
        if value is None or value == "":
            return None

        return Decimal(str(value))

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, Any],
        raw_payload: str,
    ) -> "CryptoTick":
        """Cria um objeto de domínio validado a partir de um payload JSON da Coinbase."""

        # Apenas eventos de ticker são suportados por este modelo.
        if payload.get("type") != "ticker":
            raise ValueError("O payload não é um evento de ticker.")

        # Valida o identificador do produto.
        product_id = str(payload.get("product_id", "")).strip()

        if not product_id:
            raise ValueError("O evento de ticker não possui product_id.")

        # A sequência é necessária para identificar o evento.
        if payload.get("sequence") is None:
            raise ValueError("O evento de ticker não possui sequência.")

        # O preço é o principal valor de mercado exigido.
        price = cls._decimal(payload.get("price"))

        if price is None:
            raise ValueError("O evento de cotação não possui preço.")

        # O carimbo de data/hora do evento é obrigatório.
        time_value = payload.get("time")

        if not time_value:
            raise ValueError("O evento do ticker não possui horário.")

        # Converter carimbo de data/hora ISO-8601 da Coinbase para datetime.
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

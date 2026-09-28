from datetime import datetime, timezone
from decimal import Decimal

import pytest

from coinbase_kafka_mysql.models.market_data import CryptoTick


def test_crypto_tick_from_valid_payload() -> None:
    """Deve criar um CryptoTick a partir de um payload válido da Coinbase."""
    payload = {
        "type": "ticker",
        "product_id": "BTC-USD",
        "sequence": 136498491469,
        "price": "85799",
        "open_24h": "81129.33",
        "volume_24h": "11188.56908497",
        "low_24h": "80000",
        "high_24h": "86000",
        "volume_30d": "100000",
        "best_bid": "85799.00",
        "best_ask": "85799.01",
        "last_size": "0.001",
        "time": "2026-09-21T17:19:59.721730Z",
    }

    tick = CryptoTick.from_payload(
        payload,
        raw_payload='{"type": "ticker"}',
    )

    assert tick.product_id == "BTC-USD"
    assert tick.sequence == 136498491469
    assert tick.price == Decimal("85799")
    assert tick.best_bid == Decimal("85799.00")
    assert tick.best_ask == Decimal("85799.01")
    assert tick.event_time == datetime(
        2026,
        9,
        21,
        17,
        19,
        59,
        721730,
        tzinfo=timezone.utc,
    )


def test_crypto_tick_rejects_non_ticker_payload() -> None:
    """Deve rejeitar payloads que não sejam eventos de ticker."""
    payload = {
        "type": "heartbeat",
    }

    with pytest.raises(ValueError, match="O payload não é um evento de ticker."):
        CryptoTick.from_payload(
            payload,
            raw_payload='{"type": "heartbeat"}',
        )


def test_crypto_tick_rejects_missing_product_id() -> None:
    """Deve rejeitar eventos de ticker sem um identificador de produto."""
    payload = {
        "type": "ticker",
        "sequence": 1,
        "price": "85799",
        "time": "2026-09-21T17:19:59.721730Z",
    }

    with pytest.raises(ValueError, match="product_id"):
        CryptoTick.from_payload(
            payload,
            raw_payload="{}",
        )


def test_crypto_tick_rejects_missing_price() -> None:
    """Deve rejeitar eventos de ticker sem preço."""
    payload = {
        "type": "ticker",
        "product_id": "BTC-USD",
        "sequence": 1,
        "time": "2026-09-21T17:19:59.721730Z",
    }

    with pytest.raises(ValueError, match="O evento de cotação não possui preço."):
        CryptoTick.from_payload(
            payload,
            raw_payload="{}",
        )


def test_crypto_tick_accepts_missing_optional_values() -> None:
    """Deve converter valores opcionais ausentes em None."""
    payload = {
        "type": "ticker",
        "product_id": "BTC-USD",
        "sequence": 1,
        "price": "85799",
        "time": "2026-09-21T17:19:59.721730Z",
    }

    tick = CryptoTick.from_payload(
        payload,
        raw_payload="{}",
    )

    assert tick.price == Decimal("85799")
    assert tick.open_24h is None
    assert tick.volume_24h is None
    assert tick.best_bid is None
    assert tick.best_ask is None

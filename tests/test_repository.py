from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import IntegrityError

from coinbase_kafka_mysql.database.repository import CryptoTickRepository


def test_save_returns_true_when_event_is_inserted() -> None:
    """Testa a persistência de um evento novo."""
    session = MagicMock()
    session_factory = MagicMock(return_value=session)

    repository = CryptoTickRepository(session_factory)

    tick = MagicMock()
    tick.product_id = "BTC-USD"
    tick.sequence = 12345
    tick.event_time = MagicMock()
    tick.price = 85000
    tick.open_24h = None
    tick.volume_24h = None
    tick.low_24h = None
    tick.high_24h = None
    tick.volume_30d = None
    tick.best_bid = None
    tick.best_ask = None
    tick.last_size = None
    tick.raw_payload = '{"type": "ticker"}'

    result = repository.save(tick)

    assert result is True
    session.add.assert_called_once()
    session.commit.assert_called_once()
    session.close.assert_called_once()


def test_save_returns_false_for_duplicate_event() -> None:
    """Testa se eventos duplicados são ignorados."""
    session = MagicMock()
    session.commit.side_effect = IntegrityError(
        "duplicate",
        {},
        Exception("duplicate"),
    )

    session_factory = MagicMock(return_value=session)

    repository = CryptoTickRepository(session_factory)

    tick = MagicMock()
    tick.product_id = "BTC-USD"
    tick.sequence = 12345
    tick.event_time = MagicMock()
    tick.price = 85000
    tick.open_24h = None
    tick.volume_24h = None
    tick.low_24h = None
    tick.high_24h = None
    tick.volume_30d = None
    tick.best_bid = None
    tick.best_ask = None
    tick.last_size = None
    tick.raw_payload = '{"type": "ticker"}'

    result = repository.save(tick)

    assert result is False
    session.rollback.assert_called_once()
    session.close.assert_called_once()


def test_save_propagates_unexpected_error() -> None:
    """Testa se erros inesperados são propagados."""
    session = MagicMock()
    session.commit.side_effect = RuntimeError("database unavailable")

    session_factory = MagicMock(return_value=session)

    repository = CryptoTickRepository(session_factory)

    tick = MagicMock()
    tick.product_id = "BTC-USD"
    tick.sequence = 12345
    tick.event_time = MagicMock()
    tick.price = 85000
    tick.open_24h = None
    tick.volume_24h = None
    tick.low_24h = None
    tick.high_24h = None
    tick.volume_30d = None
    tick.best_bid = None
    tick.best_ask = None
    tick.last_size = None
    tick.raw_payload = '{"type": "ticker"}'

    with pytest.raises(RuntimeError, match="database unavailable"):
        repository.save(tick)

    session.rollback.assert_called_once()
    session.close.assert_called_once()

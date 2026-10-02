from unittest.mock import MagicMock, patch

from coinbase_kafka_mysql.models.market_data import CryptoTick
from coinbase_kafka_mysql.services.producer_service import ProducerService


def create_service() -> tuple[
    ProducerService,
    MagicMock,
    MagicMock,
]:
    """Cria um ProducerService com dependências simuladas."""
    websocket_client = MagicMock()
    producer = MagicMock()

    service = ProducerService(
        websocket_client=websocket_client,
        producer=producer,
        topic="crypto-events",
    )

    return service, websocket_client, producer


def test_handle_message_publishes_valid_ticker() -> None:
    """Testa se um ticker válido é publicado no Kafka."""
    service, _, producer = create_service()

    payload = {
        "type": "ticker",
        "sequence": 123456,
        "product_id": "BTC-USD",
        "price": "85799",
        "time": "2026-10-02T18:00:00.000Z",
    }

    raw_payload = (
        '{"type": "ticker", '
        '"sequence": 123456, '
        '"product_id": "BTC-USD", '
        '"price": "85799", '
        '"time": "2026-10-02T18:00:00.000Z"}'
    )

    tick = CryptoTick.from_payload(payload, raw_payload)

    with patch.object(
        CryptoTick,
        "from_payload",
        return_value=tick,
    ):
        service._handle_message(payload, raw_payload)

    producer.publish.assert_called_once_with(
        topic="crypto-events",
        key="BTC-USD",
        payload=payload,
    )


def test_handle_message_ignores_non_ticker() -> None:
    """
    Testa se mensagens da Coinbase que não contêm símbolos de
    ativos (tickers) são ignoradas
    """
    service, _, producer = create_service()

    payload = {
        "type": "heartbeat",
    }

    service._handle_message(
        payload,
        '{"type": "heartbeat"}',
    )

    producer.publish.assert_not_called()


def test_handle_message_ignores_invalid_payload() -> None:
    """Testa se payloads de ticker inválidos são ignorados."""
    service, _, producer = create_service()

    payload = {
        "type": "ticker",
        "product_id": "BTC-USD",
    }

    with patch.object(
        CryptoTick,
        "from_payload",
        side_effect=ValueError("Invalid ticker"),
    ):
        service._handle_message(
            payload,
            '{"type": "ticker"}',
        )

    producer.publish.assert_not_called()


def test_handle_message_handles_unexpected_error() -> None:
    """Testa se erros inesperados são registrados e não se propagam."""
    service, _, producer = create_service()

    payload = {
        "type": "ticker",
        "product_id": "BTC-USD",
    }

    with patch.object(
        CryptoTick,
        "from_payload",
        side_effect=RuntimeError("Unexpected failure"),
    ):
        service._handle_message(
            payload,
            '{"type": "ticker"}',
        )

    producer.publish.assert_not_called()


def test_run_flushes_producer() -> None:
    """Verifica se o produtor é esvaziado após o streaming."""
    service, websocket_client, producer = create_service()

    service.run()

    websocket_client.stream.assert_called_once_with(
        service._handle_message,
    )

    producer.flush.assert_called_once()

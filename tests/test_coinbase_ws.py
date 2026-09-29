import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from coinbase_kafka_mysql.clients.coinbase_ws import (
    CoinbaseWebSocketClient,
)


def test_client_initialization() -> None:
    """Teste se o cliente armazena a configuração fornecida."""
    client = CoinbaseWebSocketClient(
        url="wss://example.com",
        product_id="BTC-USD",
        channel="ticker",
        reconnect_delay=10,
    )

    assert client.url == "wss://example.com"
    assert client.product_id == "BTC-USD"
    assert client.channel == "ticker"
    assert client.reconnect_delay == 10


def test_subscription_message() -> None:
    """Teste se o payload da assinatura é gerado corretamente."""
    client = CoinbaseWebSocketClient(
        url="wss://example.com",
        product_id="BTC-USD",
        channel="ticker",
    )

    result = client._subscription_message()

    payload = json.loads(result)

    assert payload == {
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["ticker"],
    }


def test_stream_calls_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Teste se as mensagens recebidas são passadas para o callback."""
    websocket_mock = MagicMock()

    websocket_mock.recv.side_effect = [
        json.dumps(
            {
                "type": "ticker",
                "product_id": "BTC-USD",
                "price": "85000.00",
            }
        ),
        KeyboardInterrupt,
    ]

    monkeypatch.setattr(
        "coinbase_kafka_mysql.clients.coinbase_ws.websocket.create_connection",
        lambda url, timeout: websocket_mock,
    )

    received_messages: list[dict[str, Any]] = []
    received_raw_messages: list[str] = []

    def on_message(
        payload: dict[str, Any],
        raw_message: str,
    ) -> None:
        """Armazene as mensagens recebidas pelo callback."""
        received_messages.append(payload)
        received_raw_messages.append(raw_message)

    client = CoinbaseWebSocketClient(
        url="wss://example.com",
        product_id="BTC-USD",
        channel="ticker",
    )

    client.stream(on_message)

    assert len(received_messages) == 1
    assert received_messages[0]["type"] == "ticker"
    assert received_messages[0]["product_id"] == "BTC-USD"
    assert received_messages[0]["price"] == "85000.00"

    assert len(received_raw_messages) == 1

    websocket_mock.send.assert_called_once()
    websocket_mock.close.assert_called_once()


def test_stream_sends_subscription(monkeypatch: pytest.MonkeyPatch) -> None:
    """Teste se a mensagem de inscrição é enviada para o WebSocket."""
    websocket_mock = MagicMock()

    websocket_mock.recv.side_effect = KeyboardInterrupt

    monkeypatch.setattr(
        "coinbase_kafka_mysql.clients.coinbase_ws.websocket.create_connection",
        lambda url, timeout: websocket_mock,
    )

    client = CoinbaseWebSocketClient(
        url="wss://example.com",
        product_id="ETH-USD",
        channel="ticker",
    )

    client.stream(lambda payload, raw_message: None)

    sent_message = websocket_mock.send.call_args.args[0]
    payload = json.loads(sent_message)

    assert payload["type"] == "subscribe"
    assert payload["product_ids"] == ["ETH-USD"]
    assert payload["channels"] == ["ticker"]


def test_stream_reconnects_after_connection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Teste se o cliente reconecta após um erro de conexão."""
    first_websocket = MagicMock()
    second_websocket = MagicMock()

    first_websocket.recv.side_effect = OSError("Connection failed")

    second_websocket.recv.side_effect = KeyboardInterrupt

    connections = iter(
        [
            first_websocket,
            second_websocket,
        ]
    )

    monkeypatch.setattr(
        "coinbase_kafka_mysql.clients.coinbase_ws.websocket.create_connection",
        lambda url, timeout: next(connections),
    )

    sleep_mock = MagicMock()

    monkeypatch.setattr(
        "coinbase_kafka_mysql.clients.coinbase_ws.time.sleep",
        sleep_mock,
    )

    client = CoinbaseWebSocketClient(
        url="wss://example.com",
        product_id="BTC-USD",
        channel="ticker",
        reconnect_delay=5,
    )

    client.stream(lambda payload, raw_message: None)

    sleep_mock.assert_called_once_with(5)

    first_websocket.close.assert_called_once()
    second_websocket.close.assert_called_once()


def test_stream_closes_connection_after_json_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Teste se um JSON inválido faz com que a conexão seja encerrada."""
    websocket_mock = MagicMock()

    websocket_mock.recv.side_effect = [
        "invalid-json",
        KeyboardInterrupt,
    ]

    monkeypatch.setattr(
        "coinbase_kafka_mysql.clients.coinbase_ws.websocket.create_connection",
        lambda url, timeout: websocket_mock,
    )

    sleep_mock = MagicMock()

    monkeypatch.setattr(
        "coinbase_kafka_mysql.clients.coinbase_ws.time.sleep",
        sleep_mock,
    )

    client = CoinbaseWebSocketClient(
        url="wss://example.com",
        product_id="BTC-USD",
        channel="ticker",
        reconnect_delay=3,
    )

    client.stream(lambda payload, raw_message: None)

    assert websocket_mock.close.call_count == 2
    sleep_mock.assert_called_once_with(3)

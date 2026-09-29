import json
import logging
import time
from collections.abc import Callable
from typing import Any

import websocket

LOGGER = logging.getLogger(__name__)


class CoinbaseWebSocketClient:
    def __init__(
        self,
        url: str,
        product_id: str,
        channel: str,
        reconnect_delay: int = 5,
    ) -> None:
        self.url = url
        self.product_id = product_id
        self.channel = channel
        self.reconnect_delay = reconnect_delay

    def _subscription_message(self) -> str:
        return json.dumps(
            {
                "type": "subscribe",
                "product_ids": [self.product_id],
                "channels": [self.channel],
            }
        )

    def stream(self, on_message: Callable[[dict[str, Any], str], None]) -> None:
        while True:
            try:
                LOGGER.info(
                    "Conectando ao Coinbase WebSocket: %s",
                    self.url,
                )

                ws = websocket.create_connection(self.url, timeout=30)
                try:
                    ws.send(self._subscription_message())
                    LOGGER.info(
                        "Subscribed to %s for %s.",
                        self.channel,
                        self.product_id,
                    )

                    while True:
                        raw_message = ws.recv()
                        if not raw_message:
                            raise ConnectionError("A Coinbase encerrou a conexão.")

                        raw_text = str(raw_message)
                        payload = json.loads(raw_text)
                        on_message(payload, raw_text)
                finally:
                    ws.close()

            except KeyboardInterrupt:
                LOGGER.info("WebSocket stream encerrado pelo usuário.")
                return
            except (OSError, websocket.WebSocketException, json.JSONDecodeError) as exc:
                LOGGER.exception("Coinbase WebSocket falhou: %s", exc)
                LOGGER.info(
                    "Reconectando em %s segundos.",
                    self.reconnect_delay,
                )
                time.sleep(self.reconnect_delay)

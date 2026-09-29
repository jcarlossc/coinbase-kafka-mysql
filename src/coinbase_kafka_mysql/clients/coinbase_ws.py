import json
import logging
import time
from collections.abc import Callable
from typing import Any

import websocket

# Cria um logger específico para este módulo.
LOGGER = logging.getLogger(__name__)


class CoinbaseWebSocketClient:
    """Ler mensagens públicas de cotação (ticker) do WebSocket da Coinbase Exchange."""

    def __init__(
        self,
        url: str,
        product_id: str,
        channel: str,
        reconnect_delay: int = 5,
    ) -> None:
        """Inicializa o WebSocket."""

        """
        Args: url: Coinbase WebSocket endpoint.
        product_id: Par de negociação, por exemplo ``BTC-USD``.
        channel: Canal WebSocket, por exemplo ``ticker``.
        reconnect_delay: Segundos de espera antes de reconectar.
        """
        self.url = url
        self.product_id = product_id
        self.channel = channel
        self.reconnect_delay = reconnect_delay

    def _subscription_message(self) -> str:
        """
        Monta o payload de assinatura da Coinbase.

        Returns:
            Uma string JSON contendo a solicitação de assinatura.
        """
        return json.dumps(
            {
                "type": "subscribe",
                "product_ids": [self.product_id],
                "channels": [self.channel],
            }
        )

    def stream(self, on_message: Callable[[dict[str, Any], str], None]) -> None:
        """
        Transmite mensagens JSON continuamente e reconecta-se após falhas..

        Args:
            on_message: Callback called with the parsed JSON
            payload and the original raw message.
        """

        # Loop principal. Mantém o cliente funcionando continuamente.
        while True:
            try:
                LOGGER.info(
                    "Conectando ao Coinbase WebSocket: %s",
                    self.url,
                )

                # Criando conexão.
                ws = websocket.create_connection(self.url, timeout=30)
                try:
                    # Envia inscrição:
                    # 1º - Constrói o JSON e
                    # 2º - envia para a Coinbase.
                    ws.send(self._subscription_message())
                    LOGGER.info(
                        "Subscribed to %s for %s.",
                        self.channel,
                        self.product_id,
                    )

                    # Loop de recebimento.
                    while True:
                        # Espera uma mensagem do servidor.
                        raw_message = ws.recv()
                        # Espera uma mensagem do servidor.
                        if not raw_message:
                            raise ConnectionError("A Coinbase encerrou a conexão.")

                        # Convertendo para string.
                        raw_text = str(raw_message)
                        # Convertendo JSON.
                        payload = json.loads(raw_text)
                        # Executando o callback:
                        # 1º - JSON convertido para Python.
                        # 2º - Mensagem original.
                        on_message(payload, raw_text)
                finally:
                    # Encerra conexão.
                    ws.close()

            except KeyboardInterrupt:
                LOGGER.info("WebSocket stream encerrado pelo usuário.")
                return
            except (OSError, websocket.WebSocketException, json.JSONDecodeError):
                LOGGER.exception("Coinbase WebSocket falhou.")
                LOGGER.info(
                    "Reconectando em %s segundos.",
                    self.reconnect_delay,
                )
                time.sleep(self.reconnect_delay)

import logging
from typing import Any

from coinbase_kafka.clients.coinbase_ws import CoinbaseWebSocketClient
from coinbase_kafka.kafka.producer import KafkaProducer
from coinbase_kafka.models.market_data import CryptoTick

LOGGER = logging.getLogger(__name__)


class ProducerService:
    def __init__(
        self,
        websocket_client: CoinbaseWebSocketClient,
        producer: KafkaProducer,
        topic: str,
    ) -> None:
        self.websocket_client = websocket_client
        self.producer = producer
        self.topic = topic

    def _handle_message(
        self,
        payload: dict[str, Any],
        raw_payload: str,
    ) -> None:
        try:
            if payload.get("type") != "ticker":
                LOGGER.debug(
                    "Ignorando mensagem da Coinbase do tipo=%s",
                    payload.get("type"),
                )
                return

            tick = CryptoTick.from_payload(payload, raw_payload)

            self.producer.publish(
                topic=self.topic,
                key=tick.product_id,
                payload=payload,
            )

        except (ValueError, TypeError) as exc:
            LOGGER.warning(
                "Evento inválido da Coinbase ignorado: %s",
                exc,
            )

        except Exception:
            LOGGER.exception(
                "Falha inesperada do serviço produtor.",
            )

    def run(self) -> None:
        try:
            self.websocket_client.stream(self._handle_message)
        finally:
            self.producer.flush()

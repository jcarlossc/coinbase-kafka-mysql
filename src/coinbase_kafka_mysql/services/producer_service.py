import logging
from typing import Any

from coinbase_kafka_mysql.clients.coinbase_ws import CoinbaseWebSocketClient
from coinbase_kafka_mysql.kafka.producer import KafkaProducer
from coinbase_kafka_mysql.models.market_data import CryptoTick

LOGGER = logging.getLogger(__name__)


class ProducerService:
    """Orquestre a ingestão via WebSocket da Coinbase e a publicação no Kafka."""

    def __init__(
        self,
        websocket_client: CoinbaseWebSocketClient,
        producer: KafkaProducer,
        topic: str,
    ) -> None:
        """
        Inicializa o serviço produtor.

        Args:
            websocket_client: Cliente responsável por receber mensagens
            WebSocket da Coinbase.
            producer: Produtor Kafka responsável por publicar eventos.
            topic: Tópico Kafka onde eventos de cotação (ticker) válidos são publicados.
        """

        self.websocket_client = websocket_client
        self.producer = producer
        self.topic = topic

    def _handle_message(
        self,
        payload: dict[str, Any],
        raw_payload: str,
    ) -> None:
        """
        Valida um ticker recebido e o publica no Kafka.

        Args:
            payload: Mensagem WebSocket da Coinbase analisada.
            raw_payload: Mensagem JSON original recebida da Coinbase.

        Returns:
            Nenhum.
        """

        try:
            # Processa apenas eventos de ticker.
            if payload.get("type") != "ticker":
                LOGGER.debug(
                    "Ignorando mensagem da Coinbase do tipo=%s",
                    payload.get("type"),
                )
                return

            # Valida e normaliza o evento de mercado recebido.
            tick = CryptoTick.from_payload(payload, raw_payload)

            # Publicar o evento validado no Kafka.
            self.producer.publish(
                topic=self.topic,
                key=tick.product_id,
                payload=payload,
            )

        except (ValueError, TypeError) as exc:
            # Dados externos inválidos não devem interromper o pipeline.
            LOGGER.warning(
                "Evento inválido da Coinbase ignorado: %s",
                exc,
            )

        except Exception:
            # Falhas inesperadas são registradas com o traceback completo.
            LOGGER.exception(
                "Falha inesperada do serviço produtor.",
            )

    def run(self) -> None:
        """
        Inicia o pipeline contínuo da Coinbase para o Kafka.

        O produtor é esvaziado (flushed) quando o processo de streaming termina,
        inclusive quando o WebSocket gera uma exceção.
        """
        try:
            self.websocket_client.stream(self._handle_message)
        finally:
            self.producer.flush()

import json
import logging
from collections.abc import Mapping
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.error import KafkaException

# Cria um logger específico para este módulo.
LOGGER = logging.getLogger(__name__)


class KafkaProducer:
    """Classe que Publiqua eventos JSON em um tópico do Apache Kafka."""

    def __init__(self, bootstrap_servers: str) -> None:
        """Inicializa o produtor Kafka.

        Args:
            bootstrap_servers: Endereço do broker Kafka, como por exemplo
                ``localhost:9092``.
        """
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "coinbase-websocket-producer",
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "snappy",
                "linger.ms": 5,
            }
        )

    def _delivery_callback(
        self,
        error: Any,
        message: Any,
    ) -> None:
        """
        Trata resultados de entrega assíncrona do Kafka.

        Args:
            error: Erro de entrega do Kafka, caso tenha ocorrido.
            message: Mensagem do Kafka associada ao resultado da entrega.
        """

        if error is not None:
            LOGGER.error(
                "Falha na entrega ao Kafka: %s",
                error,
            )
            return

        LOGGER.debug(
            "Mensagem do Kafka entregue: tópico=%s partição=%s offset=%s",
            message.topic(),
            message.partition(),
            message.offset(),
        )

    def publish(
        self,
        topic: str,
        payload: Mapping[str, Any],
        key: str,
    ) -> None:
        """
        Publica um payload JSON em um tópico do Kafka.

        Args:
            topic: Tópico do Kafka que recebe a mensagem.
            payload: Payload do evento serializável em JSON.
            key: Chave da mensagem no Kafka, tipicamente o ID do produto.

        Raises:
            KafkaException: Se o Kafka rejeitar a operação de publicação.
        """
        try:
            # Serializa o evento antes de enviá-lo para o Kafka.
            serialized_payload = json.dumps(payload)

            # Coloque a mensagem na fila para entrega assíncrona.
            self._producer.produce(
                topic=topic,
                key=key,
                value=serialized_payload,
                callback=self._delivery_callback,
            )

            # Permita que o Kafka processe callbacks de entrega.
            self._producer.poll(0)

        except BufferError as exc:
            # Tenta novamente após aguardar o buffer do produtor local.
            LOGGER.warning(
                "O buffer local do Kafka está cheio: %s",
                exc,
            )

            self._producer.flush(5)

            self._producer.produce(
                topic=topic,
                key=key,
                value=serialized_payload,
                callback=self._delivery_callback,
            )

        except KafkaException:
            # Erros do Kafka são registrados e propagados para o chamador.
            LOGGER.exception("Falha na publicação no Kafka.")
            raise

    def flush(self, timeout: float = 10.0) -> None:
        """Aguarda mensagens pendentes do produtor.

        Args:
            timeout: Tempo máximo de espera pela entrega, em segundos.
        """
        remaining = self._producer.flush(timeout)

        if remaining:
            LOGGER.error(
                "%s mensagens do Kafka não foram entregues antes do tempo limite.",
                remaining,
            )

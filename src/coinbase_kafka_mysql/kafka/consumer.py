import logging

from confluent_kafka import Consumer, KafkaException, Message

# Cria um logger específico para este módulo.
LOGGER = logging.getLogger(__name__)


class KafkaConsumer:
    """Consume mensagens de um tópico do Kafka utilizando commits de offset manuais."""

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topic: str,
    ) -> None:
        """Inicialize o consumidor Kafka."""

        self.topic = topic
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
            }
        )

    def subscribe(self) -> None:
        """Inscreve-se no tópico do Kafka configurado."""
        self._consumer.subscribe([self.topic])
        LOGGER.info("Subscribed to Kafka topic: %s", self.topic)

    def poll(self, timeout: float) -> Message | None:
        """ "Consulta o Kafka para obter uma mensagem."""
        return self._consumer.poll(timeout)

    def commit(self, message: Message) -> None:
        """Confirma o offset de uma mensagem processada com sucesso."""
        self._consumer.commit(message=message, asynchronous=False)

    def close(self) -> None:
        """Fecha o consumidor e libere os recursos de rede."""
        self._consumer.close()

    @staticmethod
    def raise_if_error(message: Message) -> None:
        """Levanta erros do Kafka retornados pelo broker."""
        if message.error():
            raise KafkaException(message.error())

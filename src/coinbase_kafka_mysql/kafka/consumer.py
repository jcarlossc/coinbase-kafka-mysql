import logging

from confluent_kafka import Consumer, KafkaException, Message

LOGGER = logging.getLogger(__name__)


class KafkaConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topic: str,
    ) -> None:
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
        self._consumer.subscribe([self.topic])
        LOGGER.info("Subscribed to Kafka topic: %s", self.topic)

    def poll(self, timeout: float) -> Message | None:
        return self._consumer.poll(timeout)

    def commit(self, message: Message) -> None:
        self._consumer.commit(message=message, asynchronous=False)

    def close(self) -> None:
        self._consumer.close()

    @staticmethod
    def raise_if_error(message: Message) -> None:
        if message.error():
            raise KafkaException(message.error())

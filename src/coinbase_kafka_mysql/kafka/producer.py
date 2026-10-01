import json
import logging
from collections.abc import Mapping
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.error import KafkaException

LOGGER = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(self, bootstrap_servers: str) -> None:
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
        if error is not None:
            LOGGER.error(
                "Kafka delivery failed: %s",
                error,
            )
            return

        LOGGER.debug(
            "Kafka message delivered: topic=%s partition=%s offset=%s",
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
        try:
            serialized_payload = json.dumps(payload)

            self._producer.produce(
                topic=topic,
                key=key,
                value=serialized_payload,
                callback=self._delivery_callback,
            )

            self._producer.poll(0)

        except BufferError as exc:
            LOGGER.warning(
                "Kafka local buffer is full: %s",
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
            LOGGER.exception("Kafka publish failed.")
            raise

    def flush(self, timeout: float = 10.0) -> None:
        remaining = self._producer.flush(timeout)

        if remaining:
            LOGGER.error(
                "%s Kafka messages were not delivered before timeout.",
                remaining,
            )

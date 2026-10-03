import json
import logging
from typing import Any

from confluent_kafka import Message

from coinbase_kafka.kafka.consumer import KafkaConsumer
from coinbase_kafka.models.market_data import CryptoTick

LOGGER = logging.getLogger(__name__)


class ConsumerService:
    def __init__(
        self,
        consumer: KafkaConsumer,
        repository: Any,
    ) -> None:
        self.consumer = consumer

        self.repository = repository

    def _process(self, message: Message) -> None:
        raw = message.value()

        if raw is None:
            raise ValueError("Kafka message has no value.")

        raw_text = raw.decode("utf-8")

        payload = json.loads(raw_text)

        tick = CryptoTick.from_payload(payload, raw_text)

        self.repository.save(tick)

    def run(self, poll_timeout: float) -> None:
        try:
            self.consumer.subscribe()

            while True:
                message = self.consumer.poll(poll_timeout)

                if message is None:
                    continue

                try:
                    self.consumer.raise_if_error(message)

                    self._process(message)

                    self.consumer.commit(message)

                except (ValueError, json.JSONDecodeError) as exc:
                    LOGGER.error(
                        "Mensagem Kafka inválida; o offset será ignorado: %s",
                        exc,
                    )

                    self.consumer.commit(message)

                except Exception:
                    LOGGER.exception(
                        "Falha no processamento da mensagem; o deslocamento não será confirmado."
                    )

        except KeyboardInterrupt:
            LOGGER.info("Consumer stopped by user.")

        finally:
            self.consumer.close()

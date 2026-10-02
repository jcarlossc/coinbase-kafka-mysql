from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.error import KafkaException

from coinbase_kafka_mysql.kafka.producer import KafkaProducer


@pytest.fixture
def producer() -> KafkaProducer:
    """Cria um KafkaProducer com um cliente Kafka simulado (mock)."""
    with patch("coinbase_kafka_mysql.kafka.producer.Producer") as producer_class:
        service = KafkaProducer("localhost:9092")
        service._producer = producer_class.return_value

        return service


def test_initializes_producer() -> None:
    """Testa se o Produtor Kafka é inicializado com as configurações esperadas."""
    with patch("coinbase_kafka_mysql.kafka.producer.Producer") as producer_class:
        KafkaProducer("localhost:9092")

    producer_class.assert_called_once_with(
        {
            "bootstrap.servers": "localhost:9092",
            "client.id": "coinbase-websocket-producer",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "snappy",
            "linger.ms": 5,
        }
    )


def test_delivery_callback_logs_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Testa se os erros de entrega são registrados."""
    producer = KafkaProducer.__new__(KafkaProducer)

    error = "Kafka delivery failed"
    message = MagicMock()

    with caplog.at_level("ERROR"):
        producer._delivery_callback(error, message)

    assert "Kafka delivery failed" in caplog.text


def test_delivery_callback_logs_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Testa se a entrega bem-sucedida é registrada."""
    producer = KafkaProducer.__new__(KafkaProducer)

    message = MagicMock()
    message.topic.return_value = "crypto-events"
    message.partition.return_value = 0
    message.offset.return_value = 10

    with caplog.at_level("DEBUG"):
        producer._delivery_callback(None, message)

    assert "Mensagem do Kafka entregue" in caplog.text
    assert "crypto-events" in caplog.text


def test_publish_serializes_payload(
    producer: KafkaProducer,
) -> None:
    """Testa se o payload é serializado como JSON."""
    payload = {
        "product_id": "BTC-USD",
        "price": "85799",
    }

    producer.publish(
        topic="crypto-events",
        payload=payload,
        key="BTC-USD",
    )

    producer._producer.produce.assert_called_once()

    call_kwargs = producer._producer.produce.call_args.kwargs

    assert call_kwargs["topic"] == "crypto-events"
    assert call_kwargs["key"] == "BTC-USD"
    assert call_kwargs["value"] == ('{"product_id": "BTC-USD", "price": "85799"}')


def test_publish_polls_producer(
    producer: KafkaProducer,
) -> None:
    """Testa se producer.poll é chamado após a publicação."""
    producer.publish(
        topic="crypto-events",
        payload={"price": "85799"},
        key="BTC-USD",
    )

    producer._producer.poll.assert_called_once_with(0)


def test_publish_handles_buffer_error(
    producer: KafkaProducer,
) -> None:
    """Testa se o BufferError aciona o flush e a nova tentativa."""
    producer._producer.produce.side_effect = [
        BufferError("Local buffer is full"),
        None,
    ]

    producer.publish(
        topic="crypto-events",
        payload={"price": "85799"},
        key="BTC-USD",
    )

    producer._producer.flush.assert_called_once_with(5)
    assert producer._producer.produce.call_count == 2


def test_publish_reraises_kafka_exception(
    producer: KafkaProducer,
) -> None:
    """Testa se a KafkaException é propagada."""
    producer._producer.produce.side_effect = KafkaException("Kafka unavailable")

    with pytest.raises(KafkaException):
        producer.publish(
            topic="crypto-events",
            payload={"price": "85799"},
            key="BTC-USD",
        )


def test_flush_without_pending_messages(
    producer: KafkaProducer,
) -> None:
    """Testa o esvaziamento do buffer após a entrega de todas as mensagens."""
    producer._producer.flush.return_value = 0

    producer.flush()

    producer._producer.flush.assert_called_once_with(10.0)


def test_flush_logs_undelivered_messages(
    producer: KafkaProducer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Testa se mensagens não entregues geram um log de erro."""
    producer._producer.flush.return_value = 3

    with caplog.at_level("ERROR"):
        producer.flush(timeout=5.0)

    assert (
        "3 mensagens do Kafka não foram entregues antes do tempo limite." in caplog.text
    )

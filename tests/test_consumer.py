from unittest.mock import MagicMock, patch

from coinbase_kafka_mysql.kafka.consumer import KafkaConsumer


@patch("coinbase_kafka_mysql.kafka.consumer.Consumer")
def test_kafka_consumer_initializes(mock_consumer: MagicMock) -> None:
    """Testa a criação do consumidor Kafka."""
    KafkaConsumer(
        bootstrap_servers="localhost:9092",
        group_id="test-group",
        topic="crypto-events",
    )

    mock_consumer.assert_called_once_with(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )


@patch("coinbase_kafka_mysql.kafka.consumer.Consumer")
def test_poll_returns_message(mock_consumer: MagicMock) -> None:
    """Testa o retorno de uma mensagem pelo método poll."""
    message = MagicMock()
    mock_consumer.return_value.poll.return_value = message

    consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        group_id="test-group",
        topic="crypto-events",
    )

    result = consumer.poll(1.0)

    assert result is message

    mock_consumer.return_value.poll.assert_called_once_with(1.0)


@patch("coinbase_kafka_mysql.kafka.consumer.Consumer")
def test_commit_message(mock_consumer: MagicMock) -> None:
    """Testa o commit manual de uma mensagem."""
    message = MagicMock()

    consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        group_id="test-group",
        topic="crypto-events",
    )

    consumer.commit(message)

    mock_consumer.return_value.commit.assert_called_once_with(
        message=message,
        asynchronous=False,
    )


@patch("coinbase_kafka_mysql.kafka.consumer.Consumer")
def test_close_consumer(mock_consumer: MagicMock) -> None:
    """Testa o fechamento do consumidor Kafka."""
    consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        group_id="test-group",
        topic="crypto-events",
    )

    consumer.close()

    mock_consumer.return_value.close.assert_called_once()

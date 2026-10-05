import json
from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaException

from coinbase_kafka_mysql.services.consumer_service import ConsumerService
from coinbase_kafka_mysql.models.market_data import CryptoTick


@pytest.fixture
def consumer() -> MagicMock:
    """Cria um consumidor Kafka simulado."""
    return MagicMock()


@pytest.fixture
def repository() -> MagicMock:
    """Cria um repositório MySQL simulado."""
    return MagicMock()


@pytest.fixture
def service(
    consumer: MagicMock,
    repository: MagicMock,
) -> ConsumerService:
    """Cria uma instância do serviço com dependências simuladas."""
    return ConsumerService(consumer, repository)


@pytest.fixture
def valid_payload() -> dict:
    """Fornece um payload válido para os testes."""
    return {
        "type": "ticker",
        "product_id": "BTC-USD",
        "price": "85000.00",
        "sequence": 12345,
        "time": "2026-10-03T12:00:00Z",
    }


def test_process_valid_message(
    service: ConsumerService,
    repository: MagicMock,
    valid_payload: dict,
) -> None:
    """Testa o processamento e a persistência de uma mensagem válida."""
    raw_text = json.dumps(valid_payload)
    message = MagicMock()
    message.value.return_value = raw_text.encode("utf-8")

    service._process(message)

    repository.save.assert_called_once()
    saved_tick = repository.save.call_args.args[0]
    assert isinstance(saved_tick, CryptoTick)


def test_process_message_without_value(
    service: ConsumerService,
    repository: MagicMock,
) -> None:
    """Testa o tratamento de uma mensagem sem conteúdo."""
    message = MagicMock()
    message.value.return_value = None

    with pytest.raises(ValueError, match="Kafka message has no value"):
        service._process(message)

    repository.save.assert_not_called()


def test_process_invalid_json(
    service: ConsumerService,
    repository: MagicMock,
) -> None:
    """Testa o tratamento de um conteúdo JSON inválido."""
    message = MagicMock()
    message.value.return_value = b"{invalid json}"

    with pytest.raises(json.JSONDecodeError):
        service._process(message)

    repository.save.assert_not_called()


def test_run_processes_and_commits_message(
    service: ConsumerService,
    consumer: MagicMock,
    repository: MagicMock,
    valid_payload: dict,
) -> None:
    """Testa o consumo, a persistência e o commit de uma mensagem."""
    message = MagicMock()
    message.value.return_value = json.dumps(valid_payload).encode("utf-8")

    # Interrompe o loop na segunda chamada de poll.
    consumer.poll.side_effect = [message, KeyboardInterrupt]

    service.run(poll_timeout=1.0)

    consumer.subscribe.assert_called_once()
    consumer.raise_if_error.assert_called_once_with(message)
    repository.save.assert_called_once()
    consumer.commit.assert_called_once_with(message)
    consumer.close.assert_called_once()


def test_run_skips_invalid_message(
    service: ConsumerService,
    consumer: MagicMock,
    repository: MagicMock,
) -> None:
    """Testa o commit de uma mensagem com JSON inválido."""
    message = MagicMock()
    message.value.return_value = b"{invalid json}"
    consumer.poll.side_effect = [message, KeyboardInterrupt]

    service.run(poll_timeout=1.0)

    repository.save.assert_not_called()
    consumer.commit.assert_called_once_with(message)
    consumer.close.assert_called_once()


def test_run_does_not_commit_on_repository_error(
    service: ConsumerService,
    consumer: MagicMock,
    repository: MagicMock,
    valid_payload: dict,
) -> None:
    """Testa que uma falha no repositório não confirma o offset."""
    message = MagicMock()
    message.value.return_value = json.dumps(valid_payload).encode("utf-8")
    repository.save.side_effect = RuntimeError("Database unavailable")
    consumer.poll.side_effect = [message, KeyboardInterrupt]

    service.run(poll_timeout=1.0)

    repository.save.assert_called_once()
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


def test_run_continues_when_no_message(
    service: ConsumerService,
    consumer: MagicMock,
) -> None:
    """Testa a continuidade do loop quando poll retorna None."""
    consumer.poll.side_effect = [None, KeyboardInterrupt]

    service.run(poll_timeout=1.0)

    assert consumer.poll.call_count == 2
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


def test_run_handles_kafka_error(
    service: ConsumerService,
    consumer: MagicMock,
) -> None:
    """Testa que uma exceção Kafka não confirma a mensagem."""
    message = MagicMock()
    consumer.raise_if_error.side_effect = KafkaException("Kafka error")
    consumer.poll.side_effect = [message, KeyboardInterrupt]

    service.run(poll_timeout=1.0)

    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()

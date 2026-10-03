import json
import logging
from typing import Any

from confluent_kafka import Message

from coinbase_kafka_mysql.kafka.consumer import KafkaConsumer
from coinbase_kafka_mysql.models.market_data import CryptoTick

# Cria um logger específico para este módulo.
LOGGER = logging.getLogger(__name__)


class ConsumerService:
    """Orquestra o consumo de eventos Kafka e a persistência no MySQL."""

    def __init__(
        self,
        consumer: KafkaConsumer,
        repository: Any,
    ) -> None:
        """Inicializa o serviço de consumo.

        Args:
            consumer: Instância responsável pela comunicação com o Kafka.
            repository: Repositório responsável por persistir os eventos.
        """

        # Armazena o consumidor Kafka.
        self.consumer = consumer

        # Armazena o repositório de persistência.
        self.repository = repository

    def _process(self, message: Message) -> None:
        """Valida, converte e persiste uma mensagem Kafka.

        Args:
            message: Mensagem recebida do tópico Kafka.

        Raises:
            ValueError: Caso a mensagem não possua conteúdo válido.
            json.JSONDecodeError: Caso o conteúdo não seja um JSON válido.
        """

        # Obtém o conteúdo bruto da mensagem.
        raw = message.value()

        # Obtém o conteúdo bruto da mensagem.
        if raw is None:
            raise ValueError("Kafka message has no value.")

        # Decodifica os bytes para uma string UTF-8.
        raw_text = raw.decode("utf-8")

        # Converte o JSON em um dicionário Python.
        payload = json.loads(raw_text)

        # Converte o JSON em um dicionário Python.
        tick = CryptoTick.from_payload(payload, raw_text)

        # Persiste o evento utilizando o repositório.
        self.repository.save(tick)

    def run(self, poll_timeout: float) -> None:
        """Executa o loop de consumo até a interrupção.

        Args:
            poll_timeout: Tempo máximo de espera por mensagem, em segundos.
        """

        try:
            # Inscreve o consumidor no tópico Kafka.
            self.consumer.subscribe()

            # Mantém o consumo enquanto o processo estiver ativo.
            while True:
                # Aguarda uma mensagem do Kafka.
                message = self.consumer.poll(poll_timeout)

                # Continua o loop se nenhuma mensagem estiver disponível.
                if message is None:
                    continue

                try:
                    # Verifica se a mensagem retornou algum erro Kafka.
                    self.consumer.raise_if_error(message)

                    # Valida e persiste a mensagem.
                    self._process(message)

                    # Confirma o offset após a persistência bem-sucedida.
                    self.consumer.commit(message)

                except (ValueError, json.JSONDecodeError) as exc:
                    # Confirma o offset após a persistência bem-sucedida.
                    LOGGER.error(
                        "Mensagem Kafka inválida; o offset será ignorado: %s",
                        exc,
                    )

                    # Confirma a mensagem inválida para evitar
                    # que ela seja consumida repetidamente.
                    self.consumer.commit(message)

                except Exception:
                    # Registra erros inesperados sem confirmar o offset.
                    LOGGER.exception(
                        "Falha no processamento da mensagem; o deslocamento não será confirmado."
                    )

        except KeyboardInterrupt:
            # Registra a interrupção solicitada pelo usuário.
            LOGGER.info("Consumer stopped by user.")

        finally:
            # Fecha o consumidor e libera os recursos.
            self.consumer.close()

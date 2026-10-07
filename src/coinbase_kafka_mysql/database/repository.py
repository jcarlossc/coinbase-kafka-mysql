import logging

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from coinbase_kafka_mysql.models.market_data import CryptoTick

from .models import CryptoTickModel

LOGGER = logging.getLogger(__name__)


class CryptoTickRepository:
    """Persiste eventos CryptoTick utilizando sessões SQLAlchemy."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        """
        Inicializa o repositório.

        Args:
            session_factory: Fábrica responsável por criar sessões SQLAlchemy.
        """

        self.session_factory = session_factory

    def save(self, tick: CryptoTick) -> bool:
        """
        Inicializa o repositório.

        Args:
            session_factory: Fábrica responsável por criar sessões SQLAlchemy.
        """

        # Cria uma nova sessão para esta operação.
        session = self.session_factory()

        try:
            # Converte o objeto de domínio para o modelo ORM.
            row = CryptoTickModel(
                product_id=tick.product_id,
                sequence=tick.sequence,
                event_time=tick.event_time,
                price=tick.price,
                open_24h=tick.open_24h,
                volume_24h=tick.volume_24h,
                low_24h=tick.low_24h,
                high_24h=tick.high_24h,
                volume_30d=tick.volume_30d,
                best_bid=tick.best_bid,
                best_ask=tick.best_ask,
                last_size=tick.last_size,
                raw_payload=tick.raw_payload,
            )

            # Adiciona o registro à transação.
            session.add(row)

            # Confirma a transação no banco.
            session.commit()

            return True

        except IntegrityError:
            # Desfaz a transação caso o registro já exista.
            session.rollback()

            LOGGER.warning(
                "Evento duplicado ignorado: product=%s sequence=%s",
                tick.product_id,
                tick.sequence,
            )

            return False

        except Exception:
            session.rollback()

            LOGGER.exception("Falha ao persistir o evento CryptoTick.")

            # Propaga o erro para a camada superior.
            raise

        finally:
            # Garante que a sessão seja sempre encerrada.
            session.close()

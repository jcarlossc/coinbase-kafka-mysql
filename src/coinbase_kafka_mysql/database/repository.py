import logging

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from coinbase_kafka_mysql.models.market_data import CryptoTick

from .models import CryptoTickModel

LOGGER = logging.getLogger(__name__)


class CryptoTickRepository:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self.session_factory = session_factory

    def save(self, tick: CryptoTick) -> bool:
        session = self.session_factory()

        try:
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

            session.add(row)

            session.commit()

            return True

        except IntegrityError:
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

            raise

        finally:
            session.close()

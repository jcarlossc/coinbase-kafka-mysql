from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import DateTime, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class CryptoTickModel(Base):
    __tablename__ = "crypto_ticks"

    __table_args__ = (
        UniqueConstraint(
            "product_id",
            "sequence",
            name="uq_crypto_ticks_product_sequence",
        ),
    )

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    product_id: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        index=True,
    )

    sequence: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    event_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    price: Mapped[Decimal] = mapped_column(
        Numeric(24, 10),
        nullable=False,
    )

    open_24h: Mapped[Decimal | None] = mapped_column(
        Numeric(24, 10),
    )

    volume_24h: Mapped[Decimal | None] = mapped_column(
        Numeric(30, 12),
    )

    low_24h: Mapped[Decimal | None] = mapped_column(
        Numeric(24, 10),
    )

    high_24h: Mapped[Decimal | None] = mapped_column(
        Numeric(24, 10),
    )

    volume_30d: Mapped[Decimal | None] = mapped_column(
        Numeric(30, 12),
    )

    best_bid: Mapped[Decimal | None] = mapped_column(
        Numeric(24, 10),
    )

    best_ask: Mapped[Decimal | None] = mapped_column(
        Numeric(24, 10),
    )

    last_size: Mapped[Decimal | None] = mapped_column(
        Numeric(30, 12),
    )

    raw_payload: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )

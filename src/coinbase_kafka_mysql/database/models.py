from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import DateTime, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Classe base para os modelos ORM da aplicação."""


class CryptoTickModel(Base):
    """Representa um evento ticker da Coinbase persistido no banco."""

    __tablename__ = "crypto_ticks"

    # Garante que a combinação produto + sequência seja única.
    __table_args__ = (
        UniqueConstraint(
            "product_id",
            "sequence",
            name="uq_crypto_ticks_product_sequence",
        ),
    )

    # Identificador interno da tabela.
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    # Identificador do produto negociado, por exemplo: BTC-USD.
    product_id: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        index=True,
    )

    # Número sequencial fornecido pelo evento da Coinbase.
    sequence: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    # Momento em que o evento ocorreu.
    event_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    # Preço da negociação.
    price: Mapped[Decimal] = mapped_column(
        Numeric(24, 10),
        nullable=False,
    )

    # Estatísticas de mercado das últimas 24 horas.
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

    # Volume acumulado dos últimos 30 dias.
    volume_30d: Mapped[Decimal | None] = mapped_column(
        Numeric(30, 12),
    )

    # Melhor preço disponível de compra.
    best_bid: Mapped[Decimal | None] = mapped_column(
        Numeric(24, 10),
    )

    # Melhor preço disponível de venda.
    best_ask: Mapped[Decimal | None] = mapped_column(
        Numeric(24, 10),
    )

    # Quantidade da última negociação.
    last_size: Mapped[Decimal | None] = mapped_column(
        Numeric(30, 12),
    )

    # Quantidade da última negociação.
    raw_payload: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    # Momento em que o registro foi criado no banco.
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )

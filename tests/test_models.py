from sqlalchemy import create_engine, inspect

from coinbase_kafka_mysql.database.models import Base


def test_crypto_ticks_table_exists() -> None:
    """Testa se a tabela crypto_ticks está registrada no metadata."""

    engine = create_engine("sqlite:///:memory:")

    Base.metadata.create_all(engine)

    inspector = inspect(engine)

    assert "crypto_ticks" in inspector.get_table_names()

    engine.dispose()


def test_crypto_ticks_columns() -> None:
    """Testa se as principais colunas existem na tabela."""

    engine = create_engine("sqlite:///:memory:")

    Base.metadata.create_all(engine)

    inspector = inspect(engine)
    columns = inspector.get_columns("crypto_ticks")

    column_names = {column["name"] for column in columns}

    assert "id" in column_names
    assert "product_id" in column_names
    assert "sequence" in column_names
    assert "event_time" in column_names
    assert "price" in column_names
    assert "raw_payload" in column_names
    assert "created_at" in column_names

    engine.dispose()


def test_crypto_ticks_unique_constraint() -> None:
    """Testa a constraint única de produto e sequência."""

    engine = create_engine("sqlite:///:memory:")

    Base.metadata.create_all(engine)

    inspector = inspect(engine)

    constraints = inspector.get_unique_constraints("crypto_ticks")

    names = {constraint["name"] for constraint in constraints}

    assert "uq_crypto_ticks_product_sequence" in names

    engine.dispose()

from unittest.mock import MagicMock, patch

from coinbase_kafka_mysql.config.Settings import Settings
from coinbase_kafka_mysql.database.engine import (
    create_database_engine,
    recreate_database,
)


def test_create_database_engine() -> None:
    """Testa a criação da engine SQLAlchemy."""
    mock_engine = MagicMock()

    with patch(
        "coinbase_kafka_mysql.database.engine.create_engine",
        return_value=mock_engine,
    ) as mock_create_engine:
        engine = create_database_engine(
            "mysql+pymysql://root:123456@localhost:3306/coinbase"
        )

    assert engine is mock_engine

    mock_create_engine.assert_called_once_with(
        "mysql+pymysql://root:123456@localhost:3306/coinbase",
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=10,
        future=True,
    )


def test_recreate_database() -> None:
    """Testa a recriação do banco de dados."""
    settings = MagicMock(spec=Settings)

    settings.mysql_host = "localhost"
    settings.mysql_port = 3306
    settings.mysql_user = "root"
    settings.mysql_password = "123456"
    settings.mysql_database = "coinbase"

    mock_engine = MagicMock()

    with patch(
        "coinbase_kafka_mysql.database.engine.create_engine",
        return_value=mock_engine,
    ):
        recreate_database(settings)

    mock_engine.begin.assert_called_once()
    mock_engine.dispose.assert_called_once()

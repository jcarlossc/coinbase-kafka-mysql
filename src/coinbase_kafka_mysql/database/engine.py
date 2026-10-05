import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from coinbase_kafka_mysql.config.Settings import Settings

# Cria um logger específico para este módulo.
LOGGER = logging.getLogger(__name__)


def create_database_engine(database_url: str) -> Engine:
    """
    Cria a engine SQLAlchemy da aplicação.

    Args:
        database_url: URL de conexão do SQLAlchemy.

    Returns:
        Engine SQLAlchemy configurada.
    """

    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=10,
        future=True,
    )


def ensure_database_exists(settings: Settings) -> None:
    """
    Cria o banco de dados MySQL configurado, caso ele não exista.

    Args:
        settings: Configurações da aplicação contendo as credenciais do MySQL.

    Raises:
        SQLAlchemyError: Se a conexão ou a criação do banco de dados falhar.
    """

    host = settings.mysql_host
    port = settings.mysql_port
    user = settings.mysql_user
    password = settings.mysql_password
    database = settings.mysql_database

    root_url = f"mysql+pymysql://{user}:{password}@{host}:{port}?charset=utf8mb4"

    engine = create_engine(root_url, future=True)

    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"CREATE DATABASE IF NOT EXISTS `{database}` "
                    "CHARACTER SET utf8mb4 "
                    "COLLATE utf8mb4_unicode_ci"
                )
            )

        LOGGER.info(
            "Banco de dados '%s' verificado com sucesso.",
            database,
        )

    finally:
        engine.dispose()

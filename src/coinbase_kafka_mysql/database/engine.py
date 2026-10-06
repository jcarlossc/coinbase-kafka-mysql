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


def recreate_database(settings: Settings) -> None:
    """
    Recria o banco de dados MySQL configurado.

    O banco é removido caso já exista e criado novamente
    com charset e collation configurados.

    Args:
        settings: Configurações da aplicação contendo as credenciais
            do MySQL.

    Raises:
        SQLAlchemyError: Se a conexão, remoção ou criação do banco
            de dados falhar.
    """

    host = settings.mysql_host
    port = settings.mysql_port
    user = settings.mysql_user
    password = settings.mysql_password
    database = settings.mysql_database

    root_url = f"mysql+pymysql://{user}:{password}@{host}:{port}?charset=utf8mb4"

    engine = create_engine(root_url)

    try:
        with engine.begin() as connection:
            connection.execute(text(f"DROP DATABASE IF EXISTS `{database}`"))

            connection.execute(
                text(
                    f"CREATE DATABASE `{database}` "
                    "CHARACTER SET utf8mb4 "
                    "COLLATE utf8mb4_unicode_ci"
                )
            )

        LOGGER.info(
            "Banco de dados '%s' recriado com sucesso.",
            database,
        )

    finally:
        engine.dispose()

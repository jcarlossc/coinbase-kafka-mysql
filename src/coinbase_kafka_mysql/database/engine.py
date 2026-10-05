import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from coinbase_kafka_mysql.config.Settings import Settings

LOGGER = logging.getLogger(__name__)


def create_database_engine(database_url: str) -> Engine:
    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=10,
        future=True,
    )


def ensure_database_exists(settings: Settings) -> None:
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
    finally:
        engine.dispose()

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_topic: str = Field(default="crypto-events")
    kafka_group_id: str = Field(default="coinbase-mysql-consumer")

    coinbase_ws_url: str = Field(default="wss://ws-feed.exchange.coinbase.com")
    coinbase_product_id: str = Field(default="BTC-USD")
    coinbase_channel: str = Field(default="ticker")
    mysql_host: str = Field(default="localhost")

    mysql_port: int = Field(default=3306)
    mysql_database: str = Field(default="crypto_database")
    mysql_user: str = Field(default="crypto_user")
    mysql_password: str = Field(default="crypto_password")

    log_level: str = Field(default="INFO")
    reconnect_delay_seconds: int = Field(default=5)
    consumer_poll_timeout_seconds: float = Field(default=1.0)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def mysql_url(self) -> str:
        return (
            f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
            "?charset=utf8mb4"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()

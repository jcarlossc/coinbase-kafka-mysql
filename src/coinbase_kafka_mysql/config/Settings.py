from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Configuração centralizada da aplicação.

    Os valores podem ser fornecidos por meio de variáveis de
    ambiente ou de um arquivo '.env' local.
    """

    # ----------------------------------
    # configurações do Kafka.
    # ----------------------------------
    # Endereço do Kafka.
    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    # O tópico que receberá os eventos.
    kafka_topic: str = Field(default="crypto-events")
    # Indentificador do Consumer Group.
    kafka_group_id: str = Field(default="coinbase-mysql-consumer")

    # ----------------------------------
    # Configurações do Coinbase.
    # ----------------------------------
    # Endereço do Websocket.
    coinbase_ws_url: str = Field(default="wss://ws-feed.exchange.coinbase.com")
    # Define qual produto será acompanhado.
    coinbase_product_id: str = Field(default="BTC-USD")
    # Define o canal de dados
    coinbase_channel: str = Field(default="ticker")

    # ----------------------------------
    # Configurações do MySQL
    # ----------------------------------
    # Servidor MySQL.
    mysql_host: str = Field(default="localhost")
    # Porta padrão do MySQL.
    mysql_port: int = Field(default=3306)
    # Nme do banco de dados.
    mysql_database: str = Field(default="crypto_database")
    # Usuário do banco de dados.
    mysql_user: str = Field(default="crypto_user")
    # Senha do banco de dados
    mysql_password: str = Field(default="crypto_password")

    # ----------------------------------
    # Configurações gerais
    # ----------------------------------
    # Define nível de logs.
    log_level: str = Field(default="INFO")
    # Tentativa de reconexão no caso do websocket cair.
    reconnect_delay_seconds: int = Field(default=5)
    # Define quanto tempo o Consumer Kafka espera por uma
    # mensagem durante o poll()
    consumer_poll_timeout_seconds: float = Field(default=1.0)

    # Configura como o Pydantic vai carregar as configurações.
    # - Procure também as configurações no arquivo .env.
    # - Define a codificação do arquivo .env.
    # - O nomes das variáveis não diferencia maiúsculas e minúsculas.
    # - Variáveis adicionais no .env que não estejam declaradas em
    # Settings, elas serão ignoradas
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def mysql_url(self) -> str:
        """Retorne a URL de conexão do SQLAlchemy para MySQL."""
        return (
            f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
            "?charset=utf8mb4"
        )


@lru_cache
def get_settings() -> Settings:
    """Retorna um objeto de configurações do aplicativo em cache."""
    return Settings()

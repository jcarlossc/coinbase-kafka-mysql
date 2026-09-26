from coinbase_kafka_mysql.config.Settings import Settings, get_settings


def test_settings_use_default_values() -> None:
    """Deve criar configurações usando os valores padrão configurados."""
    settings = Settings()

    assert settings.kafka_bootstrap_servers == "localhost:9092"
    assert settings.kafka_topic == "crypto-events"
    assert settings.coinbase_product_id == "BTC-USD"
    assert settings.mysql_port == 3306


def test_mysql_url_is_generated_correctly() -> None:
    """Deve gerar a URL de conexão do SQLAlchemy com o MySQL esperada."""
    settings = Settings(
        mysql_host="localhost",
        mysql_port=3306,
        mysql_database="test_database",
        mysql_user="test_user",
        mysql_password="test_password",
    )

    assert (
        settings.mysql_url == "mysql+pymysql://test_user:test_password"
        "@localhost:3306/test_database?charset=utf8mb4"
    )


def test_settings_can_be_loaded_from_environment(
    monkeypatch,
) -> None:
    """Deve carregar valores de configuração a partir de variáveis ​​de ambiente."""
    monkeypatch.setenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka-server:9092",
    )
    monkeypatch.setenv(
        "KAFKA_TOPIC",
        "test-topic",
    )

    settings = Settings()

    assert settings.kafka_bootstrap_servers == "kafka-server:9092"
    assert settings.kafka_topic == "test-topic"


def test_get_settings_returns_cached_instance() -> None:
    """Deve retornar a mesma instância de Settings do cache."""
    get_settings.cache_clear()

    settings_1 = get_settings()
    settings_2 = get_settings()

    assert settings_1 is settings_2

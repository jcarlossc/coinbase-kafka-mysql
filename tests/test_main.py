from coinbase_kafka_mysql.main import add_numbers


def test_add_numbers() -> None:
    """Teste"""
    result = add_numbers(10, 20)

    assert result == 30

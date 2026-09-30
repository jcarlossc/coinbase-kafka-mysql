import logging

import pytest

from coinbase_kafka_mysql.utils.loggers.logger import setup_logger


def test_setup_logger_success(monkeypatch, tmp_path):
    """
    Verifica se o logger é configurado corretamente
    quando a configuração é válida.
    """

    # Armazena os argumentos recebidos pelo basicConfig.
    called = {}

    def fake_basic_config(**kwargs):
        # Salva os parâmetros utilizados na configuração.
        called.update(kwargs)

    # Substitui logging.basicConfig pela implementação simulada.
    monkeypatch.setattr(logging, "basicConfig", fake_basic_config)

    # Arquivo temporário de log.
    log_file = tmp_path / "app.log"

    # Configuração simulada.
    config = {
        "logging": {
            "level": "INFO",
            "format": "%(levelname)s - %(message)s",
        }
    }

    # Executa a função.
    setup_logger(config, str(log_file))

    # Verifica se o nível foi configurado corretamente.
    assert called["level"] == logging.INFO

    # Verifica se o formato foi informado.
    assert called["format"] == "%(levelname)s - %(message)s"

    # Verifica se foi configurado apenas o FileHandler.
    # assert len(called["handlers"]) == 1
    assert len(called["handlers"]) == 2

    # Verifica se o handler é um FileHandler.
    assert isinstance(called["handlers"][0], logging.FileHandler)
    assert isinstance(called["handlers"][1], logging.StreamHandler)


def test_setup_logger_invalid_config(tmp_path):
    """
    Verifica se a função lança ValueError quando
    a configuração de logging é inválida.
    """

    # Configuração sem a chave "logging".
    config = {}

    log_file = tmp_path / "app.log"

    # Verifica se a exceção correta é lançada.
    with pytest.raises(ValueError, match="CONFIG_ERROR"):
        setup_logger(config, str(log_file))


def test_setup_logger_os_error(monkeypatch):
    """
    Verifica se a função relança OSError quando
    ocorre erro ao criar o arquivo de log.
    """

    # Simula falha ao criar o FileHandler.
    def fake_file_handler(*args, **kwargs):
        raise OSError("Sem permissão")

    monkeypatch.setattr(logging, "FileHandler", fake_file_handler)

    config = {
        "logging": {
            "level": "INFO",
            "format": "%(message)s",
        }
    }

    # Verifica se o erro é propagado corretamente.
    with pytest.raises(OSError, match="FILE_ERROR"):
        setup_logger(config, "logs/app.log")

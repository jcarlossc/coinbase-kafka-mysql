import logging
from pathlib import Path
from typing import Any


def setup_logger(logging_config: dict[str, Any], log_file: str) -> None:
    LOGGER = logging.getLogger(__name__)

    try:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)

        level = getattr(logging, logging_config["logging"]["level"], logging.INFO)

        logging.basicConfig(
            level=level,
            format=logging_config["logging"]["format"],
            handlers=[
                logging.FileHandler(log_file, encoding="utf-8"),
            ],
        )

        LOGGER.info("Logger configurado com sucesso.")

    except (KeyError, TypeError) as error:
        raise ValueError(f"CONFIG_ERROR: configuração inválida -> {error}") from error

    except OSError as error:
        raise OSError(
            f"FILE_ERROR: erro no arquivo de log '{log_file}' -> {error}"
        ) from error

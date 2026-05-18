import logging
import os
from utils.config import logging_folder


def get_logger(name: str = "spotify_pipeline", level: str = None) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    log_level = getattr(logging, (level or os.getenv("LOG_LEVEL", "INFO")).upper())
    logger.setLevel(log_level)

    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    os.makedirs(logging_folder, exist_ok=True)
    file_handler = logging.FileHandler(f"{logging_folder}/pipeline.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

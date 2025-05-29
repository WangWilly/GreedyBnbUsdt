import logging
import os

from pydantic import Field
from pydantic_settings import BaseSettings

################################################################################

PREFIX = "LOG_"


class Config(BaseSettings):
    DEBUG: bool = Field(
        default=False,
        alias=PREFIX + "DEBUG",
        description="Debug mode",
    )

    @property
    def default_level(self) -> int:
        return logging.DEBUG if self.DEBUG else logging.INFO


################################################################################


cfg: Config = None


def set_default_level(debug: bool):
    os.environ[PREFIX + "DEBUG"] = str(debug)


################################################################################

deep_blue = "\x1b[34;20m"
deep_green = "\x1b[32;20m"
yellow = "\x1b[33;20m"
red = "\x1b[31;20m"
bold_red = "\x1b[31;1m"
reset = "\x1b[0m"

################################################################################


class MtFormatter(logging.Formatter):
    format_pattern = (
        lambda color: f"{color}[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s]{reset} %(message)s"
    )

    FORMATS = {
        logging.DEBUG: format_pattern(deep_blue),
        logging.INFO: format_pattern(deep_green),
        logging.WARNING: format_pattern(yellow),
        logging.ERROR: format_pattern(red),
        logging.CRITICAL: format_pattern(bold_red),
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(fmt=log_fmt, datefmt="%Y-%m-%d,%H:%M:%S")
        return formatter.format(record=record)


################################################################################
# colored


def logger_default_setup(logger: logging.Logger):
    global cfg
    if cfg is None:
        cfg = Config()

    # Remove all handlers associated with the logger
    if logger.hasHandlers():
        logger.handlers.clear()

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(cfg.default_level)

    ch.setFormatter(MtFormatter())

    logger.addHandler(ch)

    logger.setLevel(cfg.default_level)


################################################################################


def get_logger_named(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger_default_setup(logger)
    return logger


def dict_format(d: dict) -> str:
    return "{" + ", ".join([f'"{k}": "{v}"' for k, v in d.items()]) + "}"


################################################################################


def get_cfg():
    global cfg
    if cfg is None:
        cfg = Config()

    return cfg

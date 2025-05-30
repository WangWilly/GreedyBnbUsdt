import json
import logging
import logging.handlers
import os

from pydantic import Field
from pydantic_settings import BaseSettings

################################################################################

PREFIX = "LOG_"


class LogConfig(BaseSettings):
    DEBUG: bool = Field(
        default=False,
        alias=PREFIX + "DEBUG",
        description="Debug mode",
    )
    FORMAT: str = Field(
        default="server",
        alias=PREFIX + "FORMAT",
        description="Log format",
    )

    FILE_PATH: str = Field(
        default="./logs/app.log",
        alias=PREFIX + "FILE_PATH",
        description="Path to the log file",
    )
    BACKUP_DAYS: int = Field(
        default=7,
        alias=PREFIX + "BACKUP_DAYS",
        description="Number of days to keep log backups",
    )

    @property
    def level(self) -> int:
        return logging.DEBUG if self.DEBUG else logging.INFO


################################################################################

deep_blue = "\x1b[34;20m"
deep_green = "\x1b[32;20m"
yellow = "\x1b[33;20m"
red = "\x1b[31;20m"
bold_red = "\x1b[31;1m"
reset = "\x1b[0m"


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

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "message": record.getMessage(),
            "level": record.levelname,
            "timestamp": self.formatTime(record, self.datefmt),
            "name": record.name,
        }
        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log_record, default=str)


################################################################################

cfg: LogConfig = None

def logger_default_setup(logger: logging.Logger):
    global cfg
    if cfg is None:
        cfg = LogConfig()

    # Remove all handlers associated with the logger
    if logger.hasHandlers():
        logger.handlers.clear()

    # create console handler with a higher log level
    if cfg.FORMAT == "json":
        log_handler = logging.StreamHandler()
        log_handler.setLevel(cfg.level)
        log_handler.setFormatter(JsonFormatter())
        logger.addHandler(log_handler)
    
    elif cfg.FORMAT == "console":
        log_handler = logging.StreamHandler()
        log_handler.setLevel(cfg.level)
        log_handler.setFormatter(MtFormatter())
        logger.addHandler(log_handler)

    elif cfg.FORMAT == "server":
        os.makedirs(os.path.dirname(cfg.FILE_PATH), exist_ok=True)
        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=cfg.FILE_PATH,
            when="midnight",
            interval=1,
            backupCount=cfg.BACKUP_DAYS,
            delay=True,
        )
        file_handler.setLevel(cfg.level)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(message)s'))
        console_handler.setLevel(cfg.level)
        logger.addHandler(console_handler)

    else:
        raise ValueError(f"Unknown log format: {cfg.FORMAT}")

    logger.setLevel(cfg.level)


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
        cfg = LogConfig()

    return cfg

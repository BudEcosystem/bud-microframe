#  -----------------------------------------------------------------------------
#  Copyright (c) 2024 Bud Ecosystem Inc.
#  #
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  #
#      http://www.apache.org/licenses/LICENSE-2.0
#  #
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#  -----------------------------------------------------------------------------

"""Provides logging utilities and preconfigured loggers for consistent and structured logging across the microservices."""

import logging
import logging.config
import warnings
from enum import Enum
from pathlib import Path
from typing import Any, Union

import pythonjsonlogger.jsonlogger


def get_logger_options(log_dir: Union[str, Path], log_level: Any) -> dict:
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json_formatter": {
                "()": pythonjsonlogger.jsonlogger.JsonFormatter,
                "format": "%(asctime)s - [%(threadName)-12.12s] [%(levelname)s] -  %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s",
            },
            "plain_formatter": {
                "()": logging.Formatter,
                "format": "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s] %(filename)s:%(lineno)d :: %(message)s",
            },
        },
        "handlers": {
            "console_plain": {
                "class": "logging.StreamHandler",
                "formatter": "plain_formatter",
            },
            "console_json": {
                "class": "logging.StreamHandler",
                "formatter": "json_formatter",
            },
            "plain_file": {
                "class": "logging.handlers.WatchedFileHandler",
                "filename": f"{log_dir.as_posix()}/app.log",
                "formatter": "plain_formatter",
            },
            "json_file": {
                "class": "logging.handlers.WatchedFileHandler",
                "filename": f"{log_dir.as_posix()}/app.log",
                "formatter": "json_formatter",
            },
        },
        "loggers": {
            "structlog": {
                "handlers": ["console_json"],
                "level": log_level,
            },
            "root": {
                "handlers": ["console_json"],
                "level": log_level,
            },
        },
    }


def skip_module_warnings_and_logs(module_names: list[str]) -> None:
    for module_name in module_names:
        logging.getLogger(module_name).setLevel(logging.WARNING)

        warnings.filterwarnings("ignore", category=UserWarning, module=module_name)


def configure_logging(log_dir: Union[str, Path], log_level: Any) -> None:
    """Configure logging settings for the application.

    Set up logging with the specified log directory and log level. This function configures the logging handlers
    and formatters to ensure that logs are written to the specified directory with the desired log level.

    Args:
        log_dir (str | Path): Directory where log files will be stored. This can be a string path or a Path object.
        log_level (Any): The log level to set for the logger. It should be one of the standard logging levels
                         such as logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, or logging.CRITICAL.

    Returns:
        None: This function does not return any value.
    """
    log_dir = Path(log_dir) if isinstance(log_dir, str) else log_dir
    log_dir.mkdir(exist_ok=True, parents=True)

    log_level = (
        log_level.value
        if isinstance(log_level, Enum)
        else log_level.upper()
        if isinstance(log_level, str)
        else log_level
    )

    logging.config.dictConfig(get_logger_options(log_dir, log_level))

    skip_module_warnings_and_logs(["urllib", "urllib3", "transformers", "sklearn"])


def get_logger(name: str) -> logging.Logger:
    """Retrieve a logger instance with the specified name.

    Obtain a `BoundLogger` instance from `structlog` with the given name. This logger can be used to log messages
    with the provided logger name.

    Args:
        name (str): The name to associate with the logger instance.

    Returns:
        logging.Logger: A `logging.Logger` instance configured with the specified name.
    """
    return logging.getLogger(name)

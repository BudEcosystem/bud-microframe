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

"""Manages application and secret configurations, utilizing environment variables and Dapr's configuration store for syncing."""

from datetime import datetime, timedelta, timezone
from distutils.util import strtobool
from pathlib import Path
from typing import Any, Dict, List, Optional

from dapr.conf import settings as dapr_settings
from dotenv import load_dotenv
from pydantic import ConfigDict, Field, model_validator
from pydantic_settings import BaseSettings

from . import logging
from .constants import Environment, LogLevel


load_dotenv("./.env")


def enable_periodic_sync_from_store(is_global: bool = False) -> Dict[str, Any]:
    """Enable periodic synchronization from the configuration store.

    Args:
        is_global (bool): Indicates if the configuration is global across all services.

    Returns:
        Dict[str, Any]: A dictionary with sync settings.
    """
    return {"sync": True, "is_global": is_global}


class BaseConfig(BaseSettings):
    """Base Config to be used as a parent class for other Config classes. Extra fields are not allowed."""

    # App Info
    name: str
    version: str
    description: str = ""
    api_root: str = ""

    model_config = ConfigDict(extra="forbid")

    max_sync_interval: int = Field(
        timedelta(hours=12).seconds,
        alias="MAX_STORE_SYNC_INTERVAL",
        ge=timedelta(hours=1).seconds,
    )

    def get_fields_to_sync(self) -> List[str]:
        """Retrieve a list of field names that are configured for synchronization with a store.

        This method inspects the fields defined in the class and checks their `json_schema_extra` attribute
        to determine if they should be synced. It collects field names based on the `sync` attribute and
        applies any `key_prefix` or `alias` settings if provided.

        Returns:
            list: A list of field names to be synced from the store. The field names are formatted
                  according to the `key_prefix` and `alias` settings if applicable.

        Example:
            ```python
            fields = instance.get_fields_to_sync()
            # Output could be something like ['description', 'config.debug', 'app.name']
            ```
        """
        fields_to_sync = []
        for name, info in self.__fields__.items():
            extra = info.json_schema_extra or {}
            if extra.get("sync") is True:
                fields_to_sync.append(
                    (f"{self.name}_" if extra.get("is_global", False) is False else "") + (info.alias or name)
                )

        return fields_to_sync

    def update_fields(self, mapping: Dict[str, Any]) -> None:
        """Update fields in the instance based on the provided mapping.

        Inspect each field defined in the class and update its value using the corresponding key in the provided
        `mapping` dictionary. The key used for lookup is determined by the field's `json_schema_extra` settings,
        applying any `key_prefix` or `alias` if specified.

        Args:
            mapping (dict): A dictionary where keys are the names of the fields to update and values are the new
                            values to assign to these fields.

        Example:
            ```python
            instance.update_fields({"description": "", "config.debug": True, "app.name": "MyApp"})
            ```
        """
        for name, info in self.__fields__.items():
            extra = info.json_schema_extra or {}
            key = (f"{self.name}_" if extra.get("is_global", False) is False else "") + (info.alias or name)
            if key in mapping:
                self.__setattr__(name, mapping[key])


class BaseAppConfig(BaseConfig):
    """Manages configuration settings for the microservice.

    This class is used to define and access the configuration settings for the microservice. It supports syncing
    fields from a dapr config store and allows configuration via environment variables.

    Attributes:
        env (str): The environment in which the application is running (e.g., 'development', 'production').
        debug (Optional[bool]): Enable or disable debugging mode.
        Other mandatory fields as required by the application.

    Sync Details:
        Fields annotated with `json_schema_extra` will be synced from the config store. The sync behavior is controlled
        by `sync=True` and additional configurations can be made using `key_prefix` and `alias`.

    Usage:
        Configure the settings via environment variables or sync with a config store. Access settings as attributes of
        an instance of `AppConfig`.

    Example:
        ```python
        from budsim.commons.config import app_settings

        if app_settings.env == "dev":
            # Development-specific logic
            ...
        ```
    """

    def __post_init__(self):
        self.psql_dbname = self.name

    # Deployment configs
    env: Environment = Field(Environment.DEVELOPMENT, alias="NAMESPACE")
    debug: Optional[bool] = Field(
        None,
        alias="DEBUG",
        json_schema_extra=enable_periodic_sync_from_store(),
    )
    log_level: Optional[LogLevel] = Field(None, alias="LOG_LEVEL")
    log_dir: Path = Field(Path("logs"), alias="LOG_DIR")

    tzone: timezone = timezone.utc
    deployed_at: datetime = datetime.now(tzone)

    # Dapr configs
    dapr_http_port: Optional[int] = Field(dapr_settings.DAPR_HTTP_PORT)
    dapr_grpc_port: Optional[int] = Field(dapr_settings.DAPR_GRPC_PORT)
    dapr_health_timeout: Optional[int] = Field(
        dapr_settings.DAPR_HEALTH_TIMEOUT,
        json_schema_extra=enable_periodic_sync_from_store(is_global=True),
    )
    dapr_api_method_invocation_protocol: Optional[str] = Field(
        "grpc", json_schema_extra=enable_periodic_sync_from_store(is_global=True)
    )

    # Config store
    configstore_name: Optional[str] = None
    config_subscription_id: Optional[str] = None

    # Secret store
    secretstore_name: Optional[str] = None
    secretstore_secret_name: Optional[str] = Field(None, alias="SECRETSTORE_SECRET_NAME")

    # State store
    statestore_name: Optional[str] = None
    cluster_info_state_key: Optional[str] = Field("cluster_info", alias="CLUSTER_INFO_STATE_KEY")

    # Pubsub
    pubsub_name: Optional[str] = None
    pubsub_topic: Optional[str] = None
    dead_letter_topic: Optional[str] = None

    # Workflow
    workflow_task_queue: str = Field("simulatorQueue", alias="WORKFLOW_TASK_QUEUE")

    # Microservice
    notify_service_name: str = Field("notify", alias="NOTIFY_SERVICE_NAME")
    notify_service_topic: Optional[str] = Field(None, alias="NOTIFY_SERVICE_TOPIC")

    # Profiling
    profiler_enabled: bool = Field(False, alias="ENABLE_PROFILER")

    # Database
    psql_host: str = Field(..., alias="PSQL_HOST")
    psql_port: int = Field(..., alias="PSQL_PORT")
    psql_dbname: str = Field(..., alias="PSQL_DB_NAME")
    psql_pool_size: int = Field(100, alias="PSQL_POOL_SIZE")
    psql_max_overflow: int = Field(50, alias="PSQL_MAX_OVERFLOW")
    psql_pool_timeout: int = Field(30, alias="PSQL_POOL_TIMEOUT")
    psql_pool_recycle: int = Field(3600, alias="PSQL_POOL_RECYCLE")
    psql_pool_pre_ping: bool = Field(True, alias="PSQL_POOL_PRE_PING")
    psql_connect_timeout: int = Field(10, alias="PSQL_CONNECT_TIMEOUT")

    @model_validator(mode="before")
    @classmethod
    def resolve_env(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert environment and namespace values in the input data to `Environment` instances.

        This method processes the provided dictionary to convert the values associated with `env` and `NAMESPACE` keys
        into `Environment` instances, if they are given as strings.

        Args:
            data (dict): A dictionary containing configuration data. It may include the keys `env` and `NAMESPACE`
                         which need to be converted to `Environment` instances.

        Returns:
            dict: The updated dictionary with `env` and `NAMESPACE` values converted to `Environment` instances.
        """
        if isinstance(data.get("env"), str):
            data["env"] = Environment.from_string(data["env"])
        elif isinstance(data.get("NAMESPACE"), str):
            data["NAMESPACE"] = Environment.from_string(data["NAMESPACE"])
        return data

    @model_validator(mode="after")
    def set_env_details(self) -> "BaseAppConfig":
        """Set environment-specific details in the configuration.

        Update the configuration attributes `log_level` and `debug` based on the values from the `env` attribute if they
        are not already set. This ensures that the configuration uses environment-specific defaults where applicable.

        Returns:
            AppConfig: The updated instance of `AppConfig` with environment details applied.
        """
        self.log_level = self.env.log_level if self.log_level is None else self.log_level
        self.debug = self.env.debug if self.debug is None else self.debug

        return self

    def __setattr__(self, name: str, value: Any) -> None:
        """Set an attribute with type conversion based on the attribute name.

        Convert the value of `log_level` to an uppercase `LogLevel` enum if it is a string, and convert the `debug`
        attribute to a boolean using `strtobool` if it is a string. For all other attributes, set the value directly
        using the superclass's `__setattr__`.

        Args:
            name (str): The name of the attribute to set.
            value (Any): The value to assign to the attribute. Type conversion is applied for specific attributes.

        Example:
            ```python
            config = AppConfig()
            config.log_level = "debug"  # This will be converted to LogLevel.DEBUG
            config.debug = "true"  # This will be converted to True
            ```
        """
        if name == "log_level" and isinstance(value, str):
            value = LogLevel(value.upper())
        elif name == "debug" and isinstance(value, str):
            value = strtobool(value)

        super().__setattr__(name, value)


class BaseSecretsConfig(BaseConfig):
    """Manages secret configurations for the microservice.

    This class handles the configuration of secrets required by the microservice. It supports secret management via
    a Dapr secret store.

    Attributes:
        dapr_api_token (str): The API token required for Dapr interactions (mandatory field).
        Other placeholder fields for secrets which can be removed or customized as needed.

    Configuration:
        Secrets should be defined in a `.env` file located in the project root. Use the prefix `SECRETS_` for syncing them with the secret store.
        For example:

        ```
        DAPR_API_TOKEN = your_api_token_here  # Won't be synced to the secret store
        SECRETS_OPENAI_TOKEN = your_api_token_here  # Will be synced to the secret store
        ```

    Sync Details:
        Similar to `AppConfig`, fields can be synced from the secret store using the `json_schema` settings.

    Usage:
        Configure secrets in `.env` for development. Ensure that the `.env` file is not included in the repository.

    Example:
        ```python
        from budsim.commons.config import secrets_settings

        api_token = secrets_settings.dapr_api_token
        ```
    """

    dapr_api_token: Optional[str] = Field(None, alias="DAPR_API_TOKEN")

    # Database
    psql_user: Optional[str] = Field(
        None,
        alias="PSQL_USER",
        json_schema_extra=enable_periodic_sync_from_store(is_global=True),
    )
    psql_password: Optional[str] = Field(
        None,
        alias="PSQL_PASSWORD",
        json_schema_extra=enable_periodic_sync_from_store(is_global=True),
    )


app_settings = None
secrets_settings = None


def register_settings(_app_settings: BaseAppConfig, _secrets_settings: BaseSecretsConfig):
    global app_settings, secrets_settings

    app_settings = _app_settings
    secrets_settings = _secrets_settings

    logging.configure_logging(app_settings.log_dir, app_settings.log_level)


def get_app_settings():
    return app_settings


def get_secrets_settings():
    return secrets_settings

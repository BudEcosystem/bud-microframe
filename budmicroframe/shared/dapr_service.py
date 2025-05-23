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

"""Provides utility functions and wrappers for interacting with Dapr components, including service invocation, pub/sub, and state management."""

import base64
import uuid
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

import ujson as json
from aiohttp import ClientConnectionError, ClientError
from dapr.clients import DaprClient
from dapr.clients.grpc._crypto import DecryptOptions, EncryptOptions
from dapr.clients.grpc._state import Concurrency, Consistency, StateOptions
from dapr.clients.grpc.client import ConfigurationResponse
from dapr.clients.grpc.interceptors import _ClientCallDetails
from dapr.conf import settings as dapr_settings
from grpc import ClientCallDetails, StreamStreamClientInterceptor

from ..commons import logging
from ..commons.config import get_app_settings, get_secrets_settings
from ..commons.exceptions import SuppressAndLog
from ..commons.resiliency import retry
from .http_client import AsyncHTTPClient


logger = logging.get_logger(__name__)


class ServiceRegistrationException(Exception):
    """Exception raised when there is an error during service registration.

    This exception is used to indicate that an attempt to register a service
    with Dapr or a service registry has failed. It can be used to catch and
    handle specific registration-related errors in the application.
    """

    pass


class DaprStreamStreamClientInterceptor(StreamStreamClientInterceptor):
    """A client interceptor for Dapr that adds an API token to the gRPC metadata."""

    def __init__(self, metadata: List[Tuple[str, str]]) -> None:
        """Initialize the interceptor with metadata.

        Args:
            metadata (List[Tuple[str, str]]): List of metadata tuples to add to gRPC calls.
        """
        self._metadata = metadata

    def _intercept_call(self, client_call_details: ClientCallDetails) -> ClientCallDetails:
        """Add metadata to gRPC metadata in the RPC call details.

        Args:
            client_call_details :class: `ClientCallDetails`: object that describes a RPC
            to be invoked

        Returns:
            :class: `ClientCallDetails` modified call details
        """
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)
        metadata.extend(self._metadata)

        new_call_details = _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )
        return new_call_details

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        """Intercept a stream-stream RPC call.

        Args:
            continuation (Callable): The continuation function to call after intercepting the call.
            client_call_details (ClientCallDetails): The details of the client call.
            request_iterator (Iterator): The request iterator to send to the continuation.

        Returns:
            Iterator: The response iterator from the continuation.
        """
        new_call_details = self._intercept_call(client_call_details)
        return continuation(new_call_details, request_iterator)


class DaprService(DaprClient):
    """A service class for interacting with Dapr, providing methods for syncing configurations and secrets.

    Inherits from:
        DaprClient: Base class for Dapr client operations.

    Args:
        api_method_invocation_protocol (Optional[str]): The protocol used for API method invocation.
            Defaults to the value in `app_settings.dapr_api_method_invocation_protocol`.
        health_timeout (Optional[int]): Timeout for health checks. Defaults to the value in
            `app_settings.dapr_health_timeout`.
        **kwargs: Additional keyword arguments passed to the `DaprClient` constructor.
    """

    def __init__(
        self,
        dapr_http_port: Optional[int] = None,
        dapr_grpc_port: Optional[int] = None,
        dapr_api_token: Optional[str] = None,
        api_method_invocation_protocol: Optional[str] = None,
        health_timeout: Optional[int] = None,
        **kwargs: Any,
    ):
        """Initialize the DaprService with optional API method invocation protocol and health timeout.

        Args:
            api_method_invocation_protocol (Optional[str]): The protocol for API method invocation.
            health_timeout (Optional[int]): Timeout for health checks.
            **kwargs: Additional keyword arguments for the `DaprClient` initialization.
        """
        app_settings = get_app_settings()
        secrets_settings = get_secrets_settings()
        if secrets_settings is not None:
            dapr_api_token = dapr_api_token or secrets_settings.dapr_api_token
        if app_settings is not None:
            dapr_http_port = dapr_http_port or app_settings.dapr_http_port
            dapr_grpc_port = dapr_grpc_port or app_settings.dapr_grpc_port
            api_method_invocation_protocol = (
                api_method_invocation_protocol or app_settings.dapr_api_method_invocation_protocol
            )
            health_timeout = health_timeout or app_settings.dapr_health_timeout
        else:
            logger.warning("App/Secrets settings are not registered, some funcionalities might not work as intended.")

        settings_updates = {
            "DAPR_HTTP_PORT": dapr_http_port,
            "DAPR_GRPC_PORT": dapr_grpc_port,
            "DAPR_API_TOKEN": dapr_api_token,
            "DAPR_API_METHOD_INVOCATION_PROTOCOL": api_method_invocation_protocol,
            "DAPR_HEALTH_TIMEOUT": health_timeout,
        }

        for attr, value in settings_updates.items():
            if value is not None:
                setattr(dapr_settings, attr, value)
                
        super().__init__(**kwargs)

    @retry(
        max_attempts=10,
        delay=1,
        backoff_factor=2,
        exceptions_to_retry=(ClientError, ClientConnectionError),
    )
    async def sync_service_metadata(self, register: bool = False) -> None:
        """Register the service with Dapr and retrieve metadata.

        This method attempts to register the service by fetching metadata from the Dapr sidecar,
        parsing the component information, and saving it to the state store. It will retry up to
        10 times with exponential backoff in case of connection errors.

        Returns:
            None

        Raises:
            ServiceRegistrationException: If the service registration fails after all retry attempts
                or if there's an error in parsing the metadata.

        Note:
            This method updates app_settings with the names of various components (configstore,
            secretstore, statestore, pubsub) and related information (pubsub topic, dead letter topic)
            based on the metadata received from Dapr.
        """
        app_settings = get_app_settings()
        secrets_settings = get_secrets_settings()
        if app_settings is None or secrets_settings is None:
            raise ServiceRegistrationException(
                "App/Secrets settings are not registered, service registration & metadata sync will not work."
            )

        async with AsyncHTTPClient(timeout=100) as client:
            response = await client.send_request(
                "GET",
                f"http://localhost:{app_settings.dapr_http_port}/v1.0/metadata",
                headers={"dapr-api-token": secrets_settings.dapr_api_token}
                if secrets_settings.dapr_api_token is not None
                else None,
                raise_for_status=False,
            )
            body = response.body.decode()
            if response.status_code != 200:
                raise ServiceRegistrationException(
                    f"Service registration failed with metadata resolution error <{response.status_code}:{body}>."
                ) from None

            metadata = json.loads(body)

        service_info: Dict[str, Optional[str]] = {
            "app_name": metadata["id"],
            "configstore": None,
            "secretstore": None,
            "statestore": None,
            "pubsub": None,
            "topic": None,
            "deadletter": None,
            "crypto": None,
        }
        try:
            for component in metadata["components"]:
                if component["type"].startswith("configuration."):
                    service_info["configstore"] = component["name"]
                    app_settings.configstore_name = component["name"]
                elif component["type"].startswith("secretstores."):
                    service_info["secretstore"] = component["name"]
                    app_settings.secretstore_name = component["name"]
                elif component["type"].startswith("state."):
                    service_info["statestore"] = component["name"]
                    app_settings.statestore_name = component["name"]
                elif component["type"].startswith("crypto."):
                    service_info["crypto"] = component["name"]
                    app_settings.crypto_name = component["name"]

            for subscription in metadata.get("subscriptions", []):
                service_info["pubsub"] = subscription["pubsubname"]
                service_info["topic"] = subscription["topic"]
                service_info["deadletter"] = subscription["deadLetterTopic"]

                app_settings.pubsub_name = subscription["pubsubname"]
                app_settings.pubsub_topic = subscription["topic"]
                app_settings.dead_letter_topic = subscription["deadLetterTopic"]
        except KeyError as e:
            raise ServiceRegistrationException(
                f"Service registration failed with metadata parse error {str(e)}."
            ) from None

        if register:
            assert service_info["statestore"], "statestore is not configured."

            failures = 0
            while failures <= 5:
                try:
                    self.save_to_statestore(
                        f"__metadata__{service_info['app_name']}",
                        service_info,
                        store_name=service_info["statestore"],
                        concurrency="first_write",
                        consistency="strong",
                    )
                    logger.info("Service registration successful.")
                    return
                except Exception as e:
                    logger.exception(f"Service registration failed with error {str(e)}.")
                    failures += 1

            raise ServiceRegistrationException("Service registration failed.")

    def get_service_metadata_by_id(self, app_id: str, store_name: Optional[str] = None) -> Dict[str, Any]:
        """Retrieve service metadata for a given application ID from the state store.

        Args:
            app_id (str): The ID of the application to retrieve metadata for.
            store_name (Optional[str]): The name of the state store to use. If not provided,
                                        it will use the default store name from app_settings.

        Returns:
            dict: The service metadata for the specified application ID.

        Raises:
            AssertionError: If the state store is not configured.

        Note:
            This method assumes that the metadata is stored with a key format of "__metadata__{app_id}".
        """
        app_settings = get_app_settings()
        store_name = store_name or app_settings.statestore_name
        assert store_name, "statestore is not configured."
        try:
            resp = self.get_state(store_name=store_name, key=f"__metadata__{app_id}")
            return json.loads(resp.data.decode("utf-8"))  # type: ignore
        except Exception as e:
            logger.exception("Failed to get service metadata: %s", str(e))

    @SuppressAndLog(Exception, _logger=logger, default_return=({}, None))
    def sync_configurations(
        self,
        keys: Union[str, List[str]],
        store_name: Optional[str] = None,
        subscription_callback: Optional[Callable[[str, ConfigurationResponse], None]] = None,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        """Sync configurations from the specified config store and optionally subscribe to configuration changes.

        Args:
            keys (str | List[str]): The configuration keys to sync.
            store_name (Optional[str]): The name of the configuration store. Defaults to `app_settings.configstore_name`.
            subscription_callback (Optional[Callable[[str, ConfigurationResponse], None]]): Optional callback
                function for handling configuration updates. If provided, will subscribe to configuration changes.

        Returns:
            Tuple[Dict[str, Any], Optional[str]]: A tuple containing a dictionary of configurations and an optional
            subscription ID. The dictionary maps keys to their corresponding configuration values.
        """
        app_settings = get_app_settings()
        store_name = store_name or app_settings.configstore_name
        assert store_name, "configstore is not configured."
        config: Dict[str, Any] = {}
        keys = [keys] if isinstance(keys, str) else keys
        try:
            configuration = self.get_configuration(store_name=store_name, keys=keys, config_metadata={})
            logger.info(
                "Found %d/%d configurations, syncing...",
                len(configuration.items),
                len(keys),
            )
            config = {key: configuration.items[key].value for key in configuration.items}
        except Exception as e:
            logger.exception("Failed to get configurations: %s", str(e))

        sub_id: Optional[str] = None
        if subscription_callback is not None:
            try:
                # FIXME: subscription gets stopped with the following message when the app receives a request
                #  configstore configuration watcher for keys ['fastapi_soa.debug'] stopped.
                sub_id = self.subscribe_configuration(
                    store_name=store_name,
                    keys=keys,
                    handler=subscription_callback,
                    config_metadata={},
                )
                logger.info(
                    "Successfully subscribed to config store with subscription id: %s",
                    sub_id,
                )
            except Exception as e:
                logger.exception("Failed to subscribe to config store: %s", str(e))

        return config, sub_id

    @SuppressAndLog(Exception, _logger=logger, default_return={})
    def sync_secrets(
        self,
        keys: Union[str, List[str]],
        store_name: Optional[str] = None,
        secret_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Sync secrets from the specified secret store.

        Args:
            keys (str | List[str]): The secret keys to sync.
            store_name (str): The name of the secret store. Defaults to `app_settings.secretstore_name`.

        Returns:
            Dict[str, Any]: A dictionary of secrets where each key maps to its corresponding secret value.
        """
        app_settings = get_app_settings()
        store_name = store_name or app_settings.secretstore_name
        secret_name = secret_name or app_settings.secretstore_secret_name
        assert store_name, "secretstore is not configured."

        secrets: Dict[str, Any] = {}
        keys = [keys] if isinstance(keys, str) else keys
        for key in keys:
            try:
                value = self.get_secret(store_name=store_name, key=secret_name or key).secret.get(key)
                if value is not None:
                    secrets[key] = value
            except Exception as e:
                logger.error("Failed to get secret: %s", str(e))

        logger.info("Found %d/%d secrets, syncing...", len(secrets), len(keys))

        return secrets

    def unsync_configurations(self, sub_id: str, store_name: Optional[str] = None) -> bool:
        """Unsubscribe from configuration updates and stop syncing.

        Args:
            store_name (str): The name of the configuration store.
            sub_id (str): The subscription ID to unsubscribe from.

        Returns:
            bool: True if successfully unsubscribed, False otherwise.
        """
        app_settings = get_app_settings()
        store_name = store_name or app_settings.configstore_name
        is_success = False

        if sub_id:
            try:
                is_success = self.unsubscribe_configuration(store_name=store_name, id=sub_id)
                logger.debug("Unsubscribed successfully? %s", is_success)
            except Exception as e:
                logger.exception("Failed to unsubscribe from config store: %s", str(e))

        return is_success

    def save_to_statestore(
        self,
        key: str,
        value: Union[Dict[str, Any], str],
        etag: Optional[str] = None,
        store_name: Optional[str] = None,
        concurrency: Literal["first_write", "last_write", "unspecified"] = "unspecified",
        consistency: Literal["eventual", "strong", "unspecified"] = "unspecified",
        ttl: Optional[int] = None,
        skip_etag_if_unset: bool = False,
    ) -> None:
        """Save a key-value pair to the state store.

        Args:
            key (str): The key to save the value under.
            value (Union[Dict[str, Any], str]): The value to save. Can be a dictionary or a string.
            etag (Optional[str]): The etag for optimistic concurrency control. If None and skip_etag_if_unset is False, it will be fetched.
            store_name (Optional[str]): The name of the state store. If None, uses the default from app settings.
            concurrency (Literal["first_write", "last_write", "unspecified"]): The concurrency mode for the operation.
            consistency (Literal["eventual", "strong", "unspecified"]): The consistency mode for the operation.
            ttl (Optional[int]): Time-to-live for the state in seconds.
            skip_etag_if_unset (bool): If True, skips fetching the etag when it's not provided.

        Raises:
            AssertionError: If the state store is not configured or if invalid concurrency or consistency options are provided.
        """
        app_settings = get_app_settings()
        store_name = store_name or app_settings.statestore_name
        assert store_name, "statestore is not configured."
        if etag is None and not skip_etag_if_unset:
            resp = self.get_state(store_name=store_name, key=key)
            etag = resp.etag

        assert concurrency is None or hasattr(
            Concurrency, concurrency
        ), f"{concurrency} is not a valid concurrency, choose from (first_write, last_write)"
        assert consistency is None or hasattr(
            Consistency, consistency
        ), f"{consistency} is not a valid consistency, choose from (eventual, strong)"

        concurrency = concurrency or "unspecified"
        consistency = consistency or "unspecified"
        state_options = StateOptions(
            concurrency=getattr(Concurrency, concurrency),
            consistency=getattr(Consistency, consistency),
        )

        state_metadata = {}
        if ttl is not None:
            state_metadata["ttlInSeconds"] = str(ttl)
        if isinstance(value, dict):
            value = json.dumps(value)
            state_metadata["contentType"] = "application/json"

        self.save_state(store_name, key, value, etag, state_options, state_metadata)

    def publish_to_topic(
        self,
        data: Dict[str, Any],
        pubsub_name: Optional[str] = None,
        target_topic_name: Optional[str] = None,
        target_name: Optional[str] = None,
        source_topic_name: Optional[str] = None,
        source_name: Optional[str] = None,
        event_type: Optional[str] = None,
    ) -> str:
        """Publish data to a specified pubsub topic.

        Args:
            data (Dict[str, Any]): The data to publish.
            pubsub_name (Optional[str]): The name of the pubsub component. If not provided, uses the default from app settings.
            target_topic_name (Optional[str]): The name of the topic to publish to. Either this or target_name must be provided.
            target_name (Optional[str]): The name of the target service. Used to resolve the topic name if target_topic_name is not provided.
            source_topic_name (Optional[str]): The name of the source topic. If not provided, uses the default from app settings.
            source_name (str): The app name of the source event. Defaults to the value of `app_settings.name`.
            event_type (Optional[str]): The type of the event. If provided, it will be included in the CloudEvent metadata.

        Returns:
            str: The ID of the published CloudEvent.

        Raises:
            DaprInternalError: If there's an error while publishing the event.
            AssertionError: If neither target_topic_name nor target_name is provided, or if pubsub is not configured.

        Note:
            - If 'workflow' is not in the data and event_type is provided, event_type is used as the workflow.
        """
        app_settings = get_app_settings()
        assert target_topic_name or target_name, "Either target_topic_name or target_name is required."
        assert source_name or (app_settings is not None and app_settings.name is not None), "Source name is not set"
        if target_topic_name is None:
            metadata = self.get_service_metadata_by_id(str(target_name))
            target_topic_name = metadata.get("topic") if isinstance(metadata, dict) else None
            assert target_topic_name, f"Failed to resolve pubsub topic for {target_name}"

        pubsub_name = pubsub_name or app_settings.pubsub_name
        source_topic_name = source_topic_name or app_settings.pubsub_topic
        assert pubsub_name, "pubsub is not configured."

        event_id = str(uuid.uuid4())
        publish_metadata = {
            "cloudevent.id": event_id,
            "cloudevent.source": source_name,
            "cloudevent.type": event_type,
        }
        publish_metadata = {k: v for k, v in publish_metadata.items() if v is not None}
        data.update({"source": source_name, "source_topic": source_topic_name})
        if data.get("type") is None and event_type is not None:
            data["type"] = event_type

        self.publish_event(
            pubsub_name=pubsub_name,
            topic_name=target_topic_name,
            data=json.dumps(data) if not isinstance(data, str) else data,
            data_content_type="application/cloudevents+json",
            publish_metadata=publish_metadata,
        )

        logger.info("Published to pubsub topic %s/%s", pubsub_name, target_topic_name)

        return event_id


class DaprServiceCrypto(DaprService):
    def __init__(
        self,
        dapr_http_port: Optional[int] = None,
        dapr_grpc_port: Optional[int] = None,
        dapr_api_token: Optional[str] = None,
        api_method_invocation_protocol: Optional[str] = None,
        health_timeout: Optional[int] = None,
        **kwargs: Any,
    ):
        secrets_settings = get_secrets_settings()
        if secrets_settings is not None:
            dapr_api_token = dapr_api_token or secrets_settings.dapr_api_token
        if dapr_api_token:
            kwargs["interceptors"] = [DaprStreamStreamClientInterceptor([("dapr-api-token", dapr_api_token)])]
        super().__init__(
            dapr_http_port=dapr_http_port,
            dapr_grpc_port=dapr_grpc_port,
            dapr_api_token=dapr_api_token,
            api_method_invocation_protocol=api_method_invocation_protocol,
            health_timeout=health_timeout,
            **kwargs,
        )
        
    def encrypt_data(self, message: str, key_wrap_algorithm: Literal["RSA", "AES"] = "RSA") -> str:
        r"""Encrypt data using the specified crypto component.

        https://github.com/dapr/python-sdk/blob/main/examples/crypto/crypto.py
        curl http://0.0.0.0:3511/v1.0/crypto/local-crypto/encrypt \
            -X PUT \
            -H "dapr-app-id: <app name>" \
            -H "dapr-api-token: <token in env>" \
            -H "dapr-key-name: <rsa key name env>" \
            -H "dapr-key-wrap-algorithm: RSA-OAEP-256" \
            -H "Content-Type: application/octet-stream" \
            --data-binary "\x68\x65\x6c\x6c\x6f\x20\x77\x6f\x72\x6c\x64"

        Args:
            message (str): The message to encrypt.

        Returns:
            str: The encrypted message.
        """
        app_settings = get_app_settings()
        options = EncryptOptions(
            component_name=app_settings.crypto_name,
            key_name=app_settings.rsa_key_name if key_wrap_algorithm == "RSA" else app_settings.aes_symmetric_key_name,
            key_wrap_algorithm=key_wrap_algorithm,
        )

        resp = self.encrypt(
            data=message.encode(),
            options=options,
        )
        encrypt_bytes: bytes = resp.read()
        return base64.b64encode(encrypt_bytes).decode("utf-8")

    def decrypt_data(self, encrypted_message: str, key_wrap_algorithm: Literal["RSA", "AES"] = "RSA") -> str:
        """Decrypt data using the specified crypto component.

        Args:
            encrypted_message (str): The encrypted message to decrypt.

        Returns:
            str: The decrypted message.
        """
        app_settings = get_app_settings()
        options = DecryptOptions(
            component_name=app_settings.crypto_name,
            key_name=app_settings.rsa_key_name if key_wrap_algorithm == "RSA" else app_settings.aes_symmetric_key_name,
        )

        # Convert base64 string back to binary data
        encrypted_bytes = base64.b64decode(encrypted_message.encode("utf-8"))

        resp = self.decrypt(
            data=encrypted_bytes,
            options=options,
        )
        decrypt_bytes: bytes = resp.read()
        return decrypt_bytes.decode("utf-8")


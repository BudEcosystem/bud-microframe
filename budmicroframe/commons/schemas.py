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

"""Contains Pydantic schemas used for data validation and serialization within the microservices."""

import datetime
import hashlib
import math
import re
import uuid
from http import HTTPStatus
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from fastapi.responses import JSONResponse
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    create_model,
    field_validator,
    model_validator,
)

from .config import get_app_settings
from .constants import NotificationCategory, NotificationType, WorkflowStatus
from .types import lowercase_string


T = TypeVar("T", bound=BaseModel)


class NotificationContent(BaseModel):
    title: str
    message: str
    status: WorkflowStatus
    result: Optional[Dict[str, Any]] = None
    primary_action: Optional[str] = None
    secondary_action: Optional[str] = None


class NotificationPayload(BaseModel):
    category: NotificationCategory = NotificationCategory.INTERNAL
    type: str
    event: str
    workflow_id: str
    source: str = ""
    content: NotificationContent

    @model_validator(mode="before")
    @classmethod
    def set_source(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        app_settings = get_app_settings()
        assert data.get("source") or app_settings.name is not None, "App name is not set"
        data["source"] = app_settings.name
        return data


class NotificationMetadata(BaseModel):
    notification_type: NotificationType = NotificationType.EVENT
    name: str
    workflow_id: str
    subscriber_ids: Optional[Union[str, List[str]]] = None
    actor: Optional[str] = None
    topic_keys: Optional[Union[str, List[str]]] = None
    category: NotificationCategory = NotificationCategory.INTERNAL


class NotificationRequest(BaseModel):
    notification_type: NotificationType = NotificationType.EVENT
    name: str
    subscriber_ids: Optional[Union[str, List[str]]] = None
    payload: NotificationPayload
    actor: Optional[str] = None
    topic_keys: Optional[Union[str, List[str]]] = None

    @classmethod
    def from_cloud_event(cls, cloud_event: "CloudEventBase", **kwargs) -> "NotificationRequest":
        name = kwargs.get("name", "")
        workflow_id = (
            cloud_event.notification_metadata.workflow_id
            if cloud_event.notification_metadata is not None
            else kwargs.get("workflow_id", "")
        )

        ntfctn_args = (
            cloud_event.notification_metadata.model_dump()
            if isinstance(cloud_event.notification_metadata, NotificationMetadata)
            else {"name": name}
        )
        ntfctn_args["payload"] = NotificationPayload(
            type=name,
            event="",
            workflow_id=workflow_id,
            content=NotificationContent(
                title="",
                message="",
                status=WorkflowStatus.PENDING,
            ),
        )
        notification_request = NotificationRequest(**ntfctn_args)
        if cloud_event.notification_metadata is not None:
            notification_request.payload.category = cloud_event.notification_metadata.category
        return notification_request

    def get_hash(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode("utf-8")).hexdigest()


class CloudEventBase(BaseModel):
    """Base class for handling HTTP requests with cloud event compatible validation.

    Configures the model to forbid extra fields not defined in the model schema.

    Attributes:
        id (str): The id of the cloud event, excluded from serialization.
        specversion (str): The spec version of the cloud event, excluded from serialization.
        datacontenttype (str): The data content type of the cloud event, excluded from serialization.
        topic (str): The topic of the cloud event, excluded from serialization.
        pubsubname (str): The pubsub name of the cloud event, excluded from serialization.
        source (str): The source of the cloud event, excluded from serialization.
        data (dict): The data of the cloud event, excluded from serialization.
        traceid (str): The trace id of the cloud event, excluded from serialization.
        tracestate (str): The trace state of the cloud event, excluded from serialization.
        traceparent (str): The trace parent of the cloud event, excluded from serialization.
        workflow (str): The type of the cloud event
        time (str): The time of the cloud event
    """

    model_config = ConfigDict(extra="forbid")

    included_fields_in_api: ClassVar[Tuple[str, ...]] = (
        "source_topic",
        "debug",
        "notification_metadata",
    )

    specversion: Optional[str] = None

    topic: Optional[str] = None
    pubsubname: Optional[str] = None
    source: Optional[str] = None
    source_topic: Optional[str] = None

    data: Optional[Dict[str, Any]] = None

    traceid: Optional[str] = None
    tracestate: Optional[str] = None
    traceparent: Optional[str] = None

    type: Optional[str] = None
    time: str = Field(default_factory=lambda: datetime.datetime.now(datetime.UTC).isoformat() + "Z")  # type: ignore
    debug: bool = Field(default=False)

    notification_metadata: Optional[NotificationMetadata] = None

    def is_pubsub(self) -> bool:
        """Check if the event is a PubSub event."""
        return self.topic is not None and self.pubsubname is not None

    @classmethod
    def create_pubsub_model(cls) -> Type[BaseModel]:
        """Create a model for PubSub with data field containing the data model.

        This method generates a new Pydantic model specifically for PubSub events. It includes all the fields
        from the CloudEventBase class as outer fields, and creates an inner model (DataModel) containing
        the fields specific to the inheriting class. The resulting model has a 'data' field that uses
        the inner model as its type.

        Returns:
            Type[BaseModel]: A new Pydantic model class for PubSub events, with a name in the format
            '{ClassName}PubSub'. This model includes all CloudEventBase fields and a 'data' field
            containing the custom fields of the inheriting class.
        """
        # Fields for the outer model (CloudEventBase fields)
        outer_fields = {
            field_name: (field_info.annotation, field_info)
            for field_name, field_info in cls.model_fields.items()
            if field_name in CloudEventBase.model_fields
        }

        # Fields for the inner model (fields specific to the inheriting class)
        inner_fields = {
            field_name: (field_info.annotation, field_info)
            for field_name, field_info in cls.model_fields.items()
            if field_name not in CloudEventBase.model_fields
        }

        # Create the inner model
        DataModel = create_model(f"{cls.__name__}Schema", **inner_fields)  # type: ignore

        # Add the data field with the inner model
        outer_fields["data"] = (DataModel, Field(...))
        return create_model(f"{cls.__name__}PubSub", **outer_fields)  # type: ignore

    @classmethod
    def create_api_model(
        cls, include: Optional[List[str]] = None, exclude: Optional[List[str]] = None
    ) -> Type[BaseModel]:
        """Create a model for API requests.

        This method generates a new Pydantic model for API requests by excluding
        certain fields from the current class. The resulting model is suitable
        for validating and serializing API request data.

        Returns:
            Type[BaseModel]: A new Pydantic model class for API requests, with
            a name in the format '{ClassName}API'.
        """
        outer_fields = {}
        fields = {}
        excluded_fields = set(CloudEventBase.model_fields) - set(include or cls.included_fields_in_api)
        if exclude:
            excluded_fields.update(exclude)

        for field_name, field_info in cls.model_fields.items():
            if field_name in CloudEventBase.model_fields and field_name not in excluded_fields:
                outer_fields[field_name] = (field_info.annotation, field_info)
            elif field_name not in excluded_fields:
                fields[field_name] = (field_info.annotation, field_info)

        fields.update(outer_fields)

        return create_model(f"{cls.__name__}Schema", **fields)  # type: ignore

    @model_validator(mode="before")
    @classmethod
    def root_validator(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and adjust the input data.

        If the `id` field and the `data` field is set, then the data field key-value pairs will be added to the instance.

        Args:
            data (dict): The input data to validate and adjust.

        Returns:
            dict: The validated and potentially adjusted data.
        """
        if all(data.get(key) is not None for key in ("topic", "pubsubname", "data")):
            data.update({k: v for k, v in data["data"].items() if k not in data})

        app_settings = get_app_settings()
        if (app_settings is None or not app_settings.debug) and data.get("debug") is True:
            data["debug"] = False

        return data


class ResponseBase(BaseModel):
    """Base class for handling HTTP responses with customizable serialization.

    Configures the model to forbid extra fields not defined in the model schema.

    Attributes:
        object (str): The type of response object, converted to lowercase.
        code (int): The HTTP status code for the response, excluded from serialization.
    """

    model_config = ConfigDict(extra="forbid")

    # TODO: Add snake case validation
    object: lowercase_string
    code: int = Field(HTTPStatus.OK.value, exclude=True)

    @staticmethod
    def to_pascal_case(string: str, prefix: Optional[str] = None, suffix: Optional[str] = None) -> str:
        """Convert a string to Pascal case.

        Transform the input string to Pascal case, with optional prefix and suffix.

        Args:
            string (str): The string to convert.
            prefix (Optional[str]): Optional prefix to add before the string.
            suffix (Optional[str]): Optional suffix to add after the string.

        Returns:
            str: The Pascal case representation of the input string.
        """
        string = (prefix or "") + string + (suffix or "")
        return re.sub(r"([_\-])+", " ", string).title().replace(" ", "")

    def to_http_response(
        self,
        include: Union[Set[int], Set[str], Dict[int, Any], Dict[str, Any], None] = None,
        exclude: Union[Set[int], Set[str], Dict[int, Any], Dict[str, Any], None] = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> JSONResponse:
        """Convert the model instance to an HTTP response.

        Serializes the model instance into a JSON response, with options to include or exclude specific fields
        and customize the response based on various parameters.

        Args:
            include (set[int] | set[str] | dict[int, Any] | dict[str, Any] | None): Fields to include in the response.
            exclude (set[int] | set[str] | dict[int, Any] | dict[str, Any] | None): Fields to exclude from the response.
            exclude_unset (bool): Whether to exclude unset fields from the response.
            exclude_defaults (bool): Whether to exclude default values from the response.
            exclude_none (bool): Whether to exclude fields with None values from the response.

        Returns:
            JSONResponse: The serialized JSON response with the appropriate status code.
        """
        if getattr(self, "object", "") == "error":
            details = self.model_dump()
            status_code = details["code"]
        else:
            details = self.model_dump(
                mode="json",
                include=include,
                exclude=exclude,
                exclude_unset=exclude_unset,
                exclude_defaults=exclude_defaults,
                exclude_none=exclude_none,
            )
            status_code = self.code

        return JSONResponse(content=details, status_code=status_code)


class SuccessResponse(ResponseBase):
    """Define a success response with optional message and parameters.

    Inherits from `ResponseBase` and specifies default values and validation for success responses.

    Attributes:
        object (str): The type of response object, defaulting to "info".
        message (Optional[str]): An optional message for the response.
    """

    object: lowercase_string = "info"
    message: Optional[str]
    param: Optional[Dict[str, Any]] = None

    @model_validator(mode="before")
    @classmethod
    def root_validator(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and set default message for the response data.

        Ensure that the `message` field is set to a default value if not provided, based on the HTTP status code.

        Args:
            data (dict): The input data to validate and adjust.

        Returns:
            dict: The validated and potentially adjusted data.
        """
        if data.get("code") is not None and data.get("message") is None:
            data["message"] = HTTPStatus(data["code"]).description

        return data


class ErrorResponse(ResponseBase):
    """Define an error response with a code and type derived from the status code.

    Inherits from `ResponseBase` and specifies the default values and validation for error responses.

    Attributes:
        object (str): The type of response object, defaulting to "error".
        message (Optional[str]): An optional message for the response.
        type (Optional[str]): The type of the error.
        param (Optional[str]): An optional parameter for additional context.
        code (int): The HTTP status code for the error.
    """

    object: lowercase_string = "error"
    message: Optional[str]
    type: Optional[str] = "InternalServerError"
    param: Optional[Dict[str, Any]] = None
    code: int = HTTPStatus.INTERNAL_SERVER_ERROR.value

    @model_validator(mode="before")
    @classmethod
    def root_validator(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and set default values for the error response data.

        Set the `type` and `message` fields based on the HTTP status code if not provided.
        The `type` is derived from the status code's phrase and suffixed with "Error" if applicable.

        Args:
            data (dict): The input data to validate and adjust.

        Returns:
            dict: The validated and potentially adjusted data.
        """
        if data.get("code") is not None:
            data["type"] = data.get("type") or cls.to_pascal_case(HTTPStatus(data["code"]).phrase, suffix="Error")
            data["message"] = data.get("message") or HTTPStatus(data["code"]).description

        return data


class PaginationRequest(BaseModel):
    """Define a paginated request with page and limit parameters.

    Attributes:
        page (int): The current page number, indicating which subset of results is being returned.
        limit (int): The maximum number of items that can be returned per page.
    """

    page: int
    limit: int


class PaginatedResponse(ResponseBase, Generic[T]):
    """Define a paginated success response with optional message and parameters.

    Attributes:
        object (str): The type of response object, defaulting to "paginated_info".
        message (Optional[str]): An optional message for the response.
        param (Optional[Dict[str, Any]]): Additional parameters for the response.
        page (int): The current page number, indicating which subset of results is being returned.
        limit (int): The maximum number of items that can be returned per page.
        total_items (int): The total number of items available across all pages.
        total_pages (int): The total number of pages available based on the limit.
        items (List[T]): The list of items for the current page.
    """

    page: int
    limit: int
    total_items: int
    total_pages: Optional[int] = None
    items: List[T]

    @model_validator(mode="after")
    def calculate_total_pages(self) -> "PaginatedResponse[T]":
        self.total_pages = math.ceil(self.total_items / self.limit)
        return self


class WorkflowStep(BaseModel):
    id: str
    title: str
    description: str


class WorkflowMetadataResponse(ResponseBase):
    object: lowercase_string = "workflow_metadata"
    workflow_id: str
    workflow_name: str
    steps: List[WorkflowStep]
    status: WorkflowStatus
    eta: Optional[int] = None

    @field_validator("workflow_id", mode="before")
    def validate_uuid(cls, value) -> str:
        try:
            uuid_obj = uuid.UUID(value)
            return str(uuid_obj)
        except ValueError:
            raise ValueError(f"Invalid UUID: {value}") from None

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

"""Defines metadata routes for the microservices, providing endpoints for retrieving service-level information."""

import uuid
from datetime import datetime

from fastapi import APIRouter, Response, status

from ..commons import logging
from ..commons.config import get_app_settings, get_secrets_settings
from ..commons.schemas import ErrorResponse, SuccessResponse
from ..shared.dapr_service import DaprService
from ..shared.dapr_workflow import DaprWorkflow, WorkflowNotFoundException


logger = logging.get_logger(__name__)

meta_router = APIRouter()


@meta_router.get(
    "/",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    description="Get microservice details.",
    tags=["Metadata"],
)
async def ping() -> Response:
    r"""Handle the endpoint to return details about the microservice.

    Calculate and return information including service name, version, description, environment, debugging status,
    deployment time, and uptime. The response is modeled using `SuccessResponse`.

    Returns:
        Response: A `SuccessResponse` containing the service information and HTTP status code 200.

    Example:
        >>> response = await ping()
        >>> response.status_code
        200
        >>> response.json()
        {
            "object": "info",
            "message": "Microservice: MyService v1.0\nDescription: A sample service\nEnvironment: DEVELOPMENT\nDebugging: Enabled\nDeployed at: 2024-01-01 12:00:00\nUptime: 1h:30m:45s"
        }
    """
    app_settings = get_app_settings()
    if app_settings is None:
        return ErrorResponse(
            message="Application is not configured properly, some settings are missing.",
            code=500,
        ).to_http_response()

    uptime_in_seconds = int((datetime.now(tz=app_settings.tzone) - app_settings.deployed_at).total_seconds())
    hours, remainder = divmod(uptime_in_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    info = (
        f"Microservice: {app_settings.name} v{app_settings.version}\n"
        f"Description: {app_settings.description}\n"
        f"Environment: {app_settings.env}\n"
        f"Debugging: {'Enabled' if app_settings.debug else 'Disabled'}\n"
        f"Deployed at: {app_settings.deployed_at}\n"
        f"Uptime: {hours}h:{minutes}m:{seconds}s"
    )

    return SuccessResponse(message=info, code=status.HTTP_200_OK).to_http_response()


@meta_router.get(
    "/health",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    description="Get microservice health.",
    tags=["Metadata"],
)
async def health() -> Response:
    """Handle the endpoint to return the health status of the microservice.

    Provides a simple acknowledgment response to indicate that the microservice is running and healthy.
    The response is modeled using `SuccessResponse`.

    Returns:
        Response: A `SuccessResponse` containing an acknowledgment message and HTTP status code 200.

    Example:
        >>> response = await health()
        >>> response.status_code
        200
        >>> response.json()
        {
            "object": "info",
            "message": "ack"
        }
    """
    return SuccessResponse(message="ack", code=status.HTTP_200_OK).to_http_response()


@meta_router.get(
    "/sync/configurations",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "Service is unavailable due to a misconfigured configuration store",
        }
    },
    description="Sync microservice configuration from a supported configstore.",
    tags=["Sync"],
)
async def sync_configurations() -> Response:
    """Synchronize the microservice configuration from a supported configstore.

    Check if a configstore is configured and syncs the microservice configuration fields from it.
    The configurations are fetched from the configstore, updated in the application settings,
    and a success message with the count of configurations synced is returned.

    Returns:
        Response: A `SuccessResponse` with the count of configurations synced and HTTP status code 200,
        or an `ErrorResponse` if the configstore is not configured, with HTTP status code 503.

    Raises:
        HTTPException: If the configstore is not configured, an HTTP 503 Service Unavailable error is returned.

    Example:
        >>> response = await sync_configurations()
        >>> response.status_code
        200
        >>> response.json()
        {
            "object": "info",
            "message": "5/10 configuration(s) synced."
        }
    """
    app_settings = get_app_settings()
    if app_settings is None:
        return ErrorResponse(
            message="Application is not configured properly, some settings are missing.",
            code=500,
        ).to_http_response()

    if app_settings.configstore_name:
        fields_to_sync = app_settings.get_fields_to_sync()

        with DaprService() as dapr_service:
            values, _ = dapr_service.sync_configurations(fields_to_sync)

        app_settings.update_fields(values)

        return SuccessResponse(
            message=f"{len(values)}/{len(fields_to_sync)} configuration(s) synced.",
            code=status.HTTP_200_OK,
        ).to_http_response()
    else:
        return ErrorResponse(
            message="Config store is not configured.",
            code=status.HTTP_503_SERVICE_UNAVAILABLE,
        ).to_http_response()


@meta_router.get(
    "/sync/secrets",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "Service is unavailable due to a misconfigured secret store",
        }
    },
    description="Sync microservice secrets from a supported secret store.",
    tags=["Sync"],
)
async def sync_secrets() -> Response:
    """Synchronize microservice secrets from a supported secret store.

    Check if a secret store is configured and syncs the microservice secret fields from it.
    The secrets are fetched from the secret store, updated in the application settings,
    and a success message with the count of secrets synced is returned.

    Returns:
        Response: A `SuccessResponse` with the count of secrets synced and HTTP status code 200,
        or an `ErrorResponse` if the secret store is not configured, with HTTP status code 503.

    Raises:
        HTTPException: If the secret store is not configured, an HTTP 503 Service Unavailable error is returned.

    Example:
        >>> response = await sync_secrets()
        >>> response.status_code
        200
        >>> response.json()
        {
            "object": "info",
            "message": "7/10 secret(s) synced."
        }
    """
    app_settings = get_app_settings()
    secrets_settings = get_secrets_settings()

    if app_settings is None or secrets_settings is None:
        return ErrorResponse(
            message="Application is not configured properly, some settings are missing.",
            code=500,
        ).to_http_response()

    if app_settings.secretstore_name:
        fields_to_sync = secrets_settings.get_fields_to_sync()

        with DaprService() as dapr_service:
            values = dapr_service.sync_secrets(fields_to_sync)

        secrets_settings.update_fields(values)

        return SuccessResponse(
            message=f"{len(values)}/{len(fields_to_sync)} secret(s) synced.",
            code=status.HTTP_200_OK,
        ).to_http_response()
    else:
        return ErrorResponse(
            message="Secret store is not configured.",
            code=status.HTTP_503_SERVICE_UNAVAILABLE,
        ).to_http_response()


@meta_router.get(
    "/register",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "model": ErrorResponse,
            "description": "Service registration failures due to internal issues.",
        }
    },
    description="Register the microservice to the ecosystem.",
    tags=["Sync"],
)
async def register_service() -> Response:
    """Register the microservice to the ecosystem.

    This endpoint attempts to register the current microservice with the ecosystem
    using the DaprService. If successful, it returns a SuccessResponse. In case of
    any failures during the registration process, it returns an ErrorResponse.

    Returns:
        Response: A SuccessResponse with HTTP status code 200 if registration is successful,
                  or an ErrorResponse with HTTP status code 500 if registration fails.

    Raises:
        HTTPException: Implicitly raised with status code 500 if an exception occurs during registration.

    Example:
        >>> response = await register_service()
        >>> response.status_code
        200
        >>> response.json()
        {
            "object": "info",
            "message": "Service registration successful."
        }
    """
    try:
        with DaprService() as dapr_service:
            await dapr_service.sync_service_metadata(register=True)

        return SuccessResponse(
            message="Service registration successful.",
            code=status.HTTP_200_OK,
        ).to_http_response()
    except Exception as e:
        logger.exception("Service registration failed with %s", str(e))

        return ErrorResponse(
            code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            message="Service registration failed.",
        ).to_http_response()


@meta_router.get("/workflow/{workflow_id}/status", tags=["Workflows"])
async def get_workflow_status(workflow_id: uuid.UUID) -> Response:
    """Retrieve the status of a specific workflow.

    This endpoint allows clients to check the current status of a workflow
    identified by the provided workflow ID. It returns the status information
    which can be used to determine if the workflow is still running, completed,
    or has encountered an error.

    Args:
        workflow_id (str): The unique identifier of the workflow whose status
        is to be retrieved.

    Returns:
        HTTP response containing the status of the specified workflow.
    """
    try:
        result = await DaprWorkflow().get_workflow_details(workflow_id=workflow_id, fetch_payloads=True)
        response = SuccessResponse(message="ack", param={"status": result["runtime_status"]}, code=200)
    except WorkflowNotFoundException:
        response = ErrorResponse(message="No such workflow exists", code=404)
    except Exception as err:
        if isinstance(err, AttributeError) and DaprWorkflow().wf_client is None:
            response = ErrorResponse(message="Workflow runtime not initialized", code=502)
        else:
            response = ErrorResponse(message="Couldn't resolve workflow status", code=500)
    return response.to_http_response()


@meta_router.delete("/workflow/{workflow_id}/stop", tags=["Workflows"])
async def stop_workflow(workflow_id: uuid.UUID) -> Response:
    """Stop a workflow by its ID."""
    response = await DaprWorkflow().stop_workflow(workflow_id)
    return response.to_http_response()


@meta_router.post("/workflow/{workflow_id}/pause", tags=["Workflows"])
async def pause_workflow(workflow_id: uuid.UUID) -> Response:
    """Pause a workflow by its ID."""
    response = await DaprWorkflow().pause_workflow(workflow_id)
    return response.to_http_response()


@meta_router.post("/workflow/{workflow_id}/resume", tags=["Workflows"])
async def resume_workflow(workflow_id: uuid.UUID) -> Response:
    """Resume a workflow by its ID."""
    response = await DaprWorkflow().resume_workflow(workflow_id)
    return response.to_http_response()


@meta_router.post("/workflow/{workflow_id}/restart", tags=["Workflows"])
async def restart_workflow(workflow_id: uuid.UUID) -> Response:
    """Restart a workflow by its ID."""
    response = await DaprWorkflow().restart_workflow(workflow_id)
    return response.to_http_response()


@meta_router.post(
    "/workflows/cleanup",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "model": ErrorResponse,
            "description": "Cleanup operation failed due to internal error.",
        }
    },
    description="Clean up old workflows from database and Dapr state store.",
    tags=["Workflows"],
)
async def cleanup_workflows(
    older_than_days: int,
    statuses: list[str] | None = None,
    limit: int | None = None,
    purge_from_dapr: bool = True,
    dry_run: bool = False,
) -> Response:
    """Clean up old workflows from both PostgreSQL database and Dapr state store.

    This endpoint removes workflow runs and their associated steps from the database
    that match the specified criteria. Optionally, it can also purge the workflow
    instances from Dapr's workflow state store (Redis/Cosmos DB/etc).

    Args:
        older_than_days: Remove workflows older than this many days (based on modified_at).
        statuses: List of workflow statuses to cleanup. If None, defaults to terminal states
                 (COMPLETED, FAILED, TERMINATED).
        limit: Maximum number of workflows to cleanup in this batch. If None, cleans all matching.
        purge_from_dapr: If True, also purge workflow instances from Dapr state store.
        dry_run: If True, only report what would be cleaned without actually deleting.

    Returns:
        Response: A SuccessResponse containing cleanup statistics with HTTP status code 200,
                  or an ErrorResponse with HTTP status code 500 if cleanup fails.

    Example:
        >>> # Preview cleanup (dry run)
        >>> response = await cleanup_workflows(older_than_days=30, dry_run=True)
        >>> response.json()
        {
            "object": "info",
            "message": "Workflow cleanup dry run completed",
            "param": {
                "workflows_scanned": 150,
                "workflows_deleted": 150,
                "steps_deleted": 450,
                "dapr_purged": 148,
                "errors": 2,
                "dry_run": True
            }
        }

        >>> # Actual cleanup
        >>> response = await cleanup_workflows(older_than_days=30, limit=100)
        >>> response.json()
        {
            "object": "info",
            "message": "Workflow cleanup completed",
            "param": {
                "workflows_scanned": 100,
                "workflows_deleted": 100,
                "steps_deleted": 300,
                "dapr_purged": 98,
                "errors": 0,
                "dry_run": False
            }
        }
    """
    try:
        workflow_manager = DaprWorkflow()
        stats = workflow_manager.cleanup_workflows(
            older_than_days=older_than_days,
            statuses=statuses,
            limit=limit,
            purge_from_dapr=purge_from_dapr,
            dry_run=dry_run,
        )

        message = "Workflow cleanup dry run completed" if dry_run else "Workflow cleanup completed"

        return SuccessResponse(message=message, param=stats, code=status.HTTP_200_OK).to_http_response()

    except Exception as e:
        logger.exception("Workflow cleanup failed: %s", str(e))
        return ErrorResponse(
            message=f"Workflow cleanup failed: {str(e)}", code=status.HTTP_500_INTERNAL_SERVER_ERROR
        ).to_http_response()


@meta_router.get(
    "/workflows/cleanup/candidates",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "model": ErrorResponse,
            "description": "Failed to retrieve cleanup candidates due to internal error.",
        }
    },
    description="Get information about workflows eligible for cleanup.",
    tags=["Workflows"],
)
async def get_cleanup_candidates(
    older_than_days: int, statuses: list[str] | None = None, limit: int | None = None
) -> Response:
    """Get information about workflows that would be cleaned up without performing deletion.

    This endpoint queries the database to find workflows matching the cleanup criteria
    and returns statistics without performing any deletion. Useful for planning cleanup
    operations and understanding what will be affected.

    Args:
        older_than_days: Check for workflows older than this many days (based on modified_at).
        statuses: List of workflow statuses to check. If None, defaults to terminal states
                 (COMPLETED, FAILED, TERMINATED).
        limit: Maximum number of workflows to include in results. If None, checks all matching.

    Returns:
        Response: A SuccessResponse containing candidate information with HTTP status code 200,
                  or an ErrorResponse with HTTP status code 500 if the operation fails.

    Example:
        >>> response = await get_cleanup_candidates(older_than_days=30)
        >>> response.json()
        {
            "object": "info",
            "message": "Found 150 workflows eligible for cleanup",
            "param": {
                "count": 150,
                "oldest_workflow_age_days": 45,
                "newest_workflow_age_days": 30,
                "by_status": {
                    "COMPLETED": 120,
                    "FAILED": 25,
                    "TERMINATED": 5
                },
                "sample_workflows": ["uuid1", "uuid2", "uuid3", ...]
            }
        }
    """
    try:
        workflow_manager = DaprWorkflow()
        candidates = workflow_manager.get_cleanup_candidates(
            older_than_days=older_than_days, statuses=statuses, limit=limit
        )

        message = f"Found {candidates['count']} workflows eligible for cleanup"

        return SuccessResponse(message=message, param=candidates, code=status.HTTP_200_OK).to_http_response()

    except Exception as e:
        logger.exception("Failed to get cleanup candidates: %s", str(e))
        return ErrorResponse(
            message=f"Failed to get cleanup candidates: {str(e)}", code=status.HTTP_500_INTERNAL_SERVER_ERROR
        ).to_http_response()

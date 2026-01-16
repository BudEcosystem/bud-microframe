# Bud Microframe

A comprehensive Python framework for building production-ready microservices with [Dapr](https://dapr.io/), FastAPI, and PostgreSQL. Bud Microframe provides pre-built components, utilities, and best practices to accelerate microservice development.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

## Features

- **Dapr Integration**: Full-featured wrapper for Dapr capabilities
  - Pub/Sub messaging with multi-topic publishing support
  - State management with consistency and concurrency control
  - Service-to-service invocation
  - Cryptography (encryption/decryption)
  - Configuration and secret management

- **Workflow Management**: Built-in support for Dapr Workflows
  - Workflow orchestration and activities
  - Progress tracking and notification system
  - Workflow state persistence

- **Database Integration**: PostgreSQL support with SQLAlchemy
  - Base models and CRUD mixins
  - Connection pooling and health checks
  - Alembic migration support
  - Database utilities (scalars, bulk operations)

- **FastAPI Foundation**: Pre-configured FastAPI application
  - Automatic service registration
  - Health check endpoints
  - Metadata routes
  - Lifespan management with config/secret syncs

- **Common Utilities**
  - Structured logging with JSON output
  - Exception handling and custom error types
  - Resiliency patterns (retry, circuit breaker)
  - Async utilities and HTTP client
  - Pydantic schemas and validators

## Installation

```bash
pip install budmicroframe
```

### Requirements

- Python 3.8+
- Dapr runtime (for Dapr features)
- PostgreSQL (for database features)

## Quick Start

### 1. Create a Basic Microservice

```python
from budmicroframe.main import create_app
from budmicroframe.commons.config import BaseAppConfig, BaseSecretsConfig
from fastapi import APIRouter

# Define your configuration
class AppConfig(BaseAppConfig):
    name: str = "my-service"
    pubsub_name: str = "pubsub"
    pubsub_topic: str = "my-topic"

class SecretsConfig(BaseSecretsConfig):
    database_url: str = "postgresql://..."

# Create FastAPI app with Dapr lifespan
app = create_app(AppConfig, SecretsConfig)

# Add your routes
router = APIRouter()

@router.get("/hello")
async def hello():
    return {"message": "Hello from my microservice!"}

app.include_router(router)
```

### 2. Run with Dapr

```bash
dapr run --app-id my-service --app-port 8000 -- uvicorn main:app --host 0.0.0.0 --port 8000
```

## Core Components

### DaprService

The `DaprService` class provides a comprehensive interface to Dapr's capabilities.

```python
from budmicroframe.shared.dapr_service import DaprService

dapr = DaprService()

# Publish to a single topic
event_id = dapr.publish_to_topic(
    data={"message": "Hello"},
    target_topic_name="my-topic",
    source_name="my-service"
)

# Publish to multiple topics (new in v0.0.1)
event_ids = dapr.publish_to_topic(
    data={"message": "Hello"},
    target_topic_name=["topic1", "topic2", "topic3"],
    source_name="my-service"
)

# Service invocation
response = dapr.invoke_method(
    app_id="other-service",
    method_name="process",
    data={"key": "value"}
)

# State management
dapr.save_state(
    store_name="statestore",
    key="my-key",
    value={"data": "value"}
)

state = dapr.get_state(
    store_name="statestore",
    key="my-key"
)
```

#### Multi-Topic Publishing

Bud Microframe supports publishing to multiple Dapr topics in a single call:

```python
from budmicroframe.shared.dapr_service import DaprService

dapr = DaprService()

# Single topic (backward compatible)
event_id = dapr.publish_to_topic(
    data={"event": "user_registered", "user_id": 123},
    target_topic_name="user-events",
    source_name="auth-service"
)
# Returns: "550e8400-e29b-41d4-a716-446655440000"

# Multiple topics (new feature)
event_ids = dapr.publish_to_topic(
    data={"event": "order_completed", "order_id": 456},
    target_topic_name=["orders-topic", "notifications-topic", "analytics-topic"],
    source_name="order-service"
)
# Returns: ["uuid1", "uuid2", "uuid3"]
```

**Key features:**
- Backward compatible - existing single-topic code works unchanged
- Type preservation - string input returns string, list input returns list
- Unique event IDs - each topic gets its own CloudEvent ID
- Fail-fast - first error aborts remaining publications
- Sequential execution - topics are published one at a time

### DaprWorkflow

Orchestrate complex workflows with state management and notifications.

```python
from budmicroframe.shared.dapr_workflow import DaprWorkflow
from dapr.ext.workflow import WorkflowActivityContext

workflow = DaprWorkflow()

# Define an activity
@workflow.activity(name="process_data")
def process_data(ctx: WorkflowActivityContext, input: dict):
    # Your processing logic
    return {"status": "processed"}

# Define a workflow
@workflow.workflow(name="my_workflow")
def my_workflow(ctx, input):
    result = yield ctx.call_activity("process_data", input=input)
    return result

# Start the workflow runtime
workflow.start_workflow_runtime()

# Schedule a workflow
workflow.schedule_new_workflow(
    workflow_name="my_workflow",
    input={"data": "value"}
)

# Publish notifications to multiple callback topics
workflow.publish_notification(
    workflow_id="wf-123",
    notification=notification_request,
    target_topic_name=["callback-topic-1", "callback-topic-2"]
)
```

### PSQLService

Database operations with PostgreSQL and SQLAlchemy.

```python
from budmicroframe.shared.psql_service import PSQLService, PSQLBase, CRUDMixin
from sqlalchemy import Column, String, Integer

# Define models
class User(PSQLBase, CRUDMixin):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    email = Column(String, unique=True)

# Use the service
psql = PSQLService()

# Health check
health = psql.health_check()

# CRUD operations
with psql.get_session() as session:
    # Create
    user = User(name="John", email="john@example.com")
    user = user.create(session)

    # Read
    users = User.get_all(session)
    user = User.get_by_id(session, user.id)

    # Update
    user.name = "Jane"
    user.update(session)

    # Delete
    user.delete(session)

# Bulk operations
psql.execute_all(session, [
    {"query": "INSERT INTO users (name, email) VALUES (:name, :email)", "params": {"name": "Alice", "email": "alice@example.com"}},
    {"query": "INSERT INTO users (name, email) VALUES (:name, :email)", "params": {"name": "Bob", "email": "bob@example.com"}}
])

# Execute scalars
results = psql.execute_scalars(session, "SELECT name FROM users WHERE active = :active", {"active": True})
```

### HTTPClient

Resilient HTTP client with retry logic and circuit breaker.

```python
from budmicroframe.shared.http_client import HTTPClient

client = HTTPClient()

# GET request
response = await client.get(
    url="https://api.example.com/data",
    headers={"Authorization": "Bearer token"}
)

# POST request
response = await client.post(
    url="https://api.example.com/create",
    json={"key": "value"}
)

# With retry and timeout
response = await client.request(
    method="GET",
    url="https://api.example.com/data",
    max_retries=3,
    timeout=10
)
```

### DaprServiceCrypto

Encryption and decryption using Dapr's cryptography component.

```python
from budmicroframe.shared.dapr_service import DaprServiceCrypto

crypto = DaprServiceCrypto()

# Encrypt data
encrypted = crypto.encrypt_data("Hello, World!")

# Decrypt data
decrypted = crypto.decrypt_data(encrypted)
assert decrypted == "Hello, World!"
```

## Configuration

### Application Settings

Create a configuration class extending `BaseAppConfig`:

```python
from budmicroframe.commons.config import BaseAppConfig
from pydantic import Field

class AppConfig(BaseAppConfig):
    # Required fields
    name: str = "my-service"

    # Dapr configuration
    pubsub_name: str = "pubsub"
    pubsub_topic: str = "my-topic"

    # Optional fields
    environment: str = "development"
    max_sync_interval: int = 300

    # Custom fields
    custom_setting: str = Field(default="value", description="My custom setting")
```

### Secrets Configuration

Create a secrets class extending `BaseSecretsConfig`:

```python
from budmicroframe.commons.config import BaseSecretsConfig

class SecretsConfig(BaseSecretsConfig):
    database_url: str
    api_key: str
    dapr_api_token: str | None = None
```

### Environment Variables

Configuration can be loaded from environment variables:

```bash
# App settings
NAME=my-service
PUBSUB_NAME=pubsub
PUBSUB_TOPIC=my-topic

# Secrets
DATABASE_URL=postgresql://user:pass@localhost/db
API_KEY=secret-key
```

## Cryptography Setup

To use encryption/decryption features:

### Step 1: Generate Keys

```bash
mkdir -p crypto-keys

# Generate RSA private key (4096-bit)
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 -out crypto-keys/rsa-private-key.pem

# Generate AES symmetric key (256-bit)
openssl rand -out crypto-keys/symmetric-key-256 32
```

### Step 2: Configure Dapr Component

Create `.dapr/components/crypto.yaml`:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: local-crypto
  namespace: development
spec:
  type: crypto.dapr.localstorage
  metadata:
    - name: version
      value: v1
    - name: path
      value: ./crypto-keys/
```

### Step 3: Set Environment Variables

Add to your `.env` file:

```bash
CRYPTO_NAME=local-crypto
RSA_KEY_NAME=rsa-private-key.pem
AES_SYMMETRIC_KEY_NAME=symmetric-key-256
```

### Step 4: Use Crypto Service

```python
from budmicroframe.shared.dapr_service import DaprServiceCrypto

crypto = DaprServiceCrypto()

# Encrypt sensitive data
encrypted_data = crypto.encrypt_data("Sensitive information")

# Decrypt when needed
decrypted_data = crypto.decrypt_data(encrypted_data)
```

## Common Utilities

### Logging

```python
from budmicroframe.commons import logging

logger = logging.get_logger(__name__)

logger.info("Service started")
logger.error("An error occurred", extra={"user_id": 123})
```

### Resiliency

```python
from budmicroframe.commons.resiliency import retry_on_exception

@retry_on_exception(max_attempts=3, delay=1.0)
async def unreliable_operation():
    # Operation that might fail
    pass
```

### Schemas

```python
from budmicroframe.commons.schemas import (
    SuccessResponse,
    ErrorResponse,
    NotificationRequest,
    WorkflowMetadataResponse
)

# Standard response formats
success = SuccessResponse(message="Operation completed", data={"id": 123})
error = ErrorResponse(error="Invalid input", details={"field": "email"})
```

## API Reference

For detailed API documentation, see the docstrings in the source code:

- **budmicroframe.shared.dapr_service**: Dapr integration
- **budmicroframe.shared.dapr_workflow**: Workflow management
- **budmicroframe.shared.psql_service**: Database operations
- **budmicroframe.shared.http_client**: HTTP client
- **budmicroframe.commons**: Utilities and common components

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/BudEcosystem/bud-microframe.git
cd bud-microframe

# Install dependencies
pip install -r requirements.txt

# Install development dependencies
pip install -e .
```

### Running Tests

```bash
pytest tests/
```

### Code Quality

```bash
# Format code
ruff format .

# Lint code
ruff check .

# Type checking
mypy budmicroframe/
```

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

Copyright (c) 2024 Bud Ecosystem Inc.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Links

- **GitHub**: https://github.com/BudEcosystem/bud-microframe
- **Issues**: https://github.com/BudEcosystem/bud-microframe/issues
- **Dapr Documentation**: https://docs.dapr.io/

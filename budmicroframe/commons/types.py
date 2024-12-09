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

"""Defines custom types and type aliases to ensure consistent data handling and type checking."""

import logging
from typing import TypeVar

from pydantic import BaseModel, StringConstraints
from typing_extensions import Annotated


Logger = logging.Logger

DBCreateSchemaType = TypeVar("DBCreateSchemaType", bound=BaseModel)
DBUpdateSchemaType = TypeVar("DBUpdateSchemaType", bound=BaseModel)

lowercase_string = Annotated[str, StringConstraints(to_lower=True)]

from datetime import UTC, datetime
from typing import Any, Dict, Generic, List, Literal, Optional, Tuple, Type, TypeVar, Union

from pydantic import PostgresDsn
from sqlalchemy import BigInteger as SqlAlchemyBigInteger
from sqlalchemy import DateTime, asc, cast, create_engine, desc, func, inspect, text
from sqlalchemy import String as SqlAlchemyString
from sqlalchemy.dialects.postgresql import ARRAY as PostgresArray
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Mapped, Session, mapped_column, scoped_session, sessionmaker
from sqlalchemy.sql import Executable

from ..commons import logging
from ..commons.config import get_app_settings, get_secrets_settings
from ..commons.exceptions import DatabaseException
from ..commons.singleton import Singleton
from ..commons.types import DBCreateSchemaType, DBUpdateSchemaType


logger = logging.get_logger(__name__)

PSQLBase = declarative_base()

ModelType = TypeVar("ModelType", bound=PSQLBase)  # type: ignore


class Database(metaclass=Singleton):
    __slots__ = ("engine", "Session", "is_connected", "connect_kwargs")

    def __init__(self):
        self.is_connected = False

    def connect(
        self,
        dbname: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        pool_size: Optional[int] = None,
        max_overflow: Optional[int] = None,
        pool_timeout: Optional[int] = None,
        pool_recycle: Optional[int] = None,
        pool_pre_ping: Optional[bool] = None,
        connect_timeout: Optional[int] = None,
        connection_scheme: str = "postgresql+psycopg",
    ):
        app_settings = get_app_settings()
        secrets_settings = get_secrets_settings()

        if app_settings is not None:
            dbname = dbname or app_settings.psql_dbname
            host = host or app_settings.psql_host
            port = port or app_settings.psql_port
            pool_size = pool_size or app_settings.psql_pool_size
            max_overflow = max_overflow or app_settings.psql_max_overflow
            pool_timeout = pool_timeout or app_settings.psql_pool_timeout
            pool_recycle = pool_recycle or app_settings.psql_pool_recycle
            pool_pre_ping = pool_pre_ping or app_settings.psql_pool_pre_ping
            connect_timeout = connect_timeout or app_settings.psql_connect_timeout

        if secrets_settings is not None:
            user = user or secrets_settings.psql_user
            password = password or secrets_settings.psql_password

        if host is None or port is None or dbname is None:
            raise ValueError(
                "Database connection details are required"
                if app_settings is not None and secrets_settings is not None
                else "App/Secrets settings are not registered, database connection details needs to be provided"
            )

        if not self.is_connected:
            try:
                db_url = PostgresDsn.build(
                    scheme=connection_scheme,
                    username=user,
                    password=password,
                    host=host,
                    port=port,
                    path=dbname,
                ).__str__()
                self.engine = create_engine(
                    db_url,
                    pool_size=pool_size,
                    max_overflow=max_overflow,
                    pool_timeout=pool_timeout,
                    pool_recycle=pool_recycle,
                    pool_pre_ping=pool_pre_ping,
                    connect_args={"connect_timeout": connect_timeout},
                )
                self.Session = scoped_session(sessionmaker(bind=self.engine))
                logger.info("Database engine created successfully")
                self.is_connected = self.check_connection()
            except SQLAlchemyError as e:
                logger.exception("Failed to create database engine: %s", str(e))
                raise RuntimeError("Could not create database engine") from e

    def check_connection(self) -> bool:
        try:
            with self.Session() as session:
                session.execute(text("SELECT 1"))
            logger.debug("Database connection established")
            return True
        except SQLAlchemyError as e:
            logger.exception("Database connection error: %s", str(e))
            return False

    def __del__(self):
        self.Session.remove()

    def get_session(self, **connect_kwargs) -> "Session":
        self.connect(**connect_kwargs)
        return self.Session()

    def close_session(self, session: "Session"):
        session.close()


class DBSession:
    """Provides instance of database session."""
    __slots__ = ("database", "session")
    
    def __init__(self, database: Optional[Database] = None):
        self.database = database or Database()
        self.session = None

    def __enter__(self):
        self.session = self.database.get_session()
        return self.session

    def __exit__(self, exc_type, exc_value, traceback):
        if self.session:
            self.database.close_session(self.session)
            self.session = None
        if exc_type:
            logger.error("Failed to close database session: %s", exc_value)


class CRUDMixin(Generic[ModelType, DBCreateSchemaType, DBUpdateSchemaType]):
    __slots__ = ("database", "session", "model")

    def __init__(self, model: Type[ModelType], database: Optional[Database] = None):
        self.model = model
        self.database = database or Database()
        self.session = None

    def __enter__(self):
        self.session = self.get_session()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup_session(self.session)
        self.session = None
        if exc_type:
            logger.error("Failed to close database session: %s", exc_value)

    def get_session(self):
        return self.session or self.database.get_session()

    def cleanup_session(self, session: Optional[Session] = None):
        if session is not None:
            self.database.close_session(session)

    @staticmethod
    async def validate_fields(model: Type[ModelType], fields: Dict[str, Any]) -> None:
        """Validate that the given fields exist in the SQLAlchemy model.

        Args:
            model (Type[DeclarativeBase]): The SQLAlchemy model class to validate against.
            fields (Dict[str, Any]): A dictionary of field names and their values to validate.

        Raises:
            DatabaseException: If an invalid field is found in the input.
        """
        for field in fields:
            if not hasattr(model, field):
                logger.error(f"Invalid field: '{field}' not found in {model.__name__} model")
                raise DatabaseException(f"Invalid field: '{field}' not found in {model.__name__} model")

    @staticmethod
    async def generate_search_stmt(model: Type[ModelType], fields: Dict[str, Any]) -> List[Executable]:
        """Generate search conditions for a SQLAlchemy model based on the provided fields.

        Args:
            model (Type[DeclarativeBase]): The SQLAlchemy model class to generate search conditions for.
            fields (Dict): A dictionary of field names and their values to search by.

        Returns:
            List[Executable]: A list of SQLAlchemy search conditions.
        """
        # Inspect model columns
        model_columns = inspect(model).columns

        # Initialize list to store search conditions
        search_conditions = []

        # Iterate over search fields and generate conditions
        for field, value in fields.items():
            column = getattr(model, field)

            # Check if column type is string like
            if type(model_columns[field].type) is SqlAlchemyString:
                search_conditions.append(func.lower(column).like(f"%{value.lower()}%"))
            elif type(model_columns[field].type) is PostgresArray:
                search_conditions.append(column.contains(value))
            elif type(model_columns[field].type) is SqlAlchemyBigInteger:
                search_conditions.append(cast(column, SqlAlchemyString).like(f"%{value}%"))
            else:
                search_conditions.append(column == value)

        return search_conditions

    @staticmethod
    async def generate_sorting_stmt(
        model: Type[ModelType], sort_details: List[Tuple[str, str]]
    ) -> List[Executable]:
        """Generate sorting conditions for a SQLAlchemy model based on the provided sort details.

        Args:
            model (Type[ModelType]): The SQLAlchemy model class to generate sorting conditions for.
            sort_details (List[Tuple[str, str]]): A list of tuples, where each tuple contains a field name and a direction ('asc' or 'desc').

        Returns:
            List[Executable]: A list of SQLAlchemy sorting conditions.
        """
        sort_conditions = []

        for field, direction in sort_details:
            # Check if column exists, if not, skip
            try:
                _ = getattr(model, field)
            except AttributeError:
                continue

            if direction == "asc":
                sort_conditions.append(getattr(model, field))
            else:
                sort_conditions.append(getattr(model, field).desc())

        return sort_conditions

    def insert(
        self,
        data: Union[DBCreateSchemaType, ModelType, Dict[str, Any]],
        session: Optional[Session] = None,
        raise_on_error: bool = True,
    ):
        _session = session or self.get_session()
        try:
            if isinstance(data, type(DBCreateSchemaType)):
                obj: ModelType = self.model(**data.model_dump())
            elif isinstance(data, dict):
                obj: ModelType = self.model(**data)
            elif isinstance(data, self.model):
                obj = data
            else:
                raise ValueError("Invalid data type for insert")

            _session.add(obj)
            _session.commit()
            _session.refresh(obj)
            logger.debug("Data inserted successfully into %s", self.model.__tablename__)
            return obj
        except SQLAlchemyError as e:
            _session.rollback()
            logger.exception("Failed to insert data into %s: %s", self.model.__tablename__, str(e))
            if raise_on_error:
                raise ValueError(f"Failed to insert data into {self.model.__tablename__}") from e
        finally:
            self.cleanup_session(_session if session is None else None)

    def fetch_one(
        self,
        conditions: Dict[str, Any] = None,
        session: Optional[Session] = None,
        order_by: Optional[List[Tuple[str, Literal["asc", "desc"]]]] = None,
        raise_on_error: bool = True,
    ):
        _session = session or self.get_session()
        try:
            query = _session.query(self.model)
            if conditions is not None:
                query = query.filter_by(**conditions)
            if order_by is not None:
                query = query.order_by(
                    *[
                        asc(getattr(self.model, col)) if order == "asc" else desc(getattr(self.model, col))
                        for col, order in order_by
                    ]
                )
            result = query.one_or_none()
            logger.debug("Single data retrieved successfully from %s", self.model.__tablename__)
            return result
        except SQLAlchemyError as e:
            logger.exception("Failed to read single data from %s: %s", self.model.__tablename__, str(e))
            if raise_on_error:
                raise ValueError(f"Failed to read single data from {self.model.__tablename__}") from e
        finally:
            self.cleanup_session(_session if session is None else None)

    def fetch_many(
        self,
        conditions: Dict[str, Any] = None,
        session: Optional[Session] = None,
        order_by: Optional[List[Tuple[str, Literal["asc", "desc"]]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        raise_on_error: bool = True,
    ):
        _session = session or self.get_session()
        try:
            query = _session.query(self.model)
            if conditions is not None:
                query = query.filter_by(**conditions)
            if order_by is not None:
                query = query.order_by(
                    *[
                        asc(getattr(self.model, col)) if order == "asc" else desc(getattr(self.model, col))
                        for col, order in order_by
                    ]
                )
            if offset is not None:
                query = query.offset(offset)
            if limit is not None:
                query = query.limit(limit)
            results = query.all()
            total_count = query.count()
            logger.debug("Data retrieved successfully from %s", self.model.__tablename__)
            return results, total_count
        except SQLAlchemyError as e:
            logger.exception("Failed to read data from %s: %s", self.model.__tablename__, str(e))
            if raise_on_error:
                raise ValueError(f"Failed to read data from {self.model.__tablename__}") from e
        finally:
            self.cleanup_session(_session if session is None else None)

    def update(
        self,
        data: Union[DBUpdateSchemaType, ModelType, Dict],
        conditions: Optional[Dict[str, Any]] = None,
        session: Optional[Session] = None,
        raise_on_error: bool = True,
    ):
        _session = session or self.get_session()
        try:
            if isinstance(data, (type(DBUpdateSchemaType), dict)):
                obj: dict = data.copy() if isinstance(data, dict) else data.model_dump(exclude_unset=True)
            elif isinstance(data, self.model):
                obj = {column.name: getattr(data, column.name) for column in data.__table__.columns}
            else:
                raise ValueError("Invalid data type for update")

            query = _session.query(self.model)
            if conditions is not None:
                query = query.filter_by(**conditions)

            result = query.update(obj)
            _session.commit()
            logger.debug(
                "Data updated successfully in %s. Total records updated: %s", self.model.__tablename__, result
            )
            return result
        except SQLAlchemyError as e:
            _session.rollback()
            logger.exception("Failed to update data in %s: %s", self.model.__tablename__, str(e))
            if raise_on_error:
                raise ValueError(f"Failed to update data in {self.model.__tablename__}") from e
        finally:
            self.cleanup_session(_session if session is None else None)

    def delete(self, conditions: Dict[str, Any], session: Optional[Session] = None, raise_on_error: bool = True):
        _session = session or self.get_session()
        try:
            deleted_count = _session.query(self.model).filter_by(**conditions).delete()
            _session.commit()
            logger.debug(
                "Data deleted successfully from %s. Total records deleted: %d", self.model.__tablename__, deleted_count
            )
        except SQLAlchemyError as e:
            _session.rollback()
            logger.exception("Failed to delete data from %s: %s", self.model.__tablename__, str(e))
            if raise_on_error:
                raise ValueError(f"Failed to delete data from {self.model.__tablename__}") from e
        finally:
            self.cleanup_session(_session if session is None else None)

    def upsert(
        self,
        data: Union[DBCreateSchemaType, ModelType, Dict],
        conflict_target: Optional[List[str]] = None,
        session: Optional[Session] = None,
        raise_on_error: bool = True,
    ):
        _session = session or self.get_session()
        try:
            if isinstance(data, (type(DBCreateSchemaType), self.model, dict)):
                obj: dict = data.copy() if isinstance(data, dict) else data.model_dump(exclude_unset=True)
            else:
                raise ValueError("Invalid data type for upsert")

            stmt = insert(self.model.__table__).values(obj)
            if conflict_target:
                stmt = stmt.on_conflict_do_update(index_elements=conflict_target, set_=obj)
            _session.execute(stmt)
            _session.commit()
            logger.debug("Upsert operation successful on %s", self.model.__tablename__)
        except SQLAlchemyError as e:
            _session.rollback()
            logger.exception("Failed to upsert data in %s: %s", self.model.__tablename__, str(e))
            if raise_on_error:
                raise ValueError(f"Failed to upsert data in {self.model.__tablename__}") from e
        finally:
            self.cleanup_session(_session if session is None else None)

    def bulk_insert(self, data: List[Dict[str, Any]], session: Optional[Session] = None, raise_on_error: bool = True):
        _session = session or self.get_session()
        try:
            if isinstance(data[0], self.model):
                _session.add_all(data)
            elif isinstance(data[0], dict):
                _session.bulk_insert_mappings(self.model, data)
            else:
                raise ValueError("Invalid data type for bulk insert")
            _session.commit()
            logger.debug("Bulk insert successful into %s", self.model.__tablename__)
        except SQLAlchemyError as e:
            _session.rollback()
            logger.exception("Failed to perform bulk insert into %s: %s", self.model.__tablename__, str(e))
            if raise_on_error:
                raise ValueError(f"Failed to perform bulk insert into {self.model.__tablename__}") from e
        finally:
            self.cleanup_session(_session if session is None else None)

    def execute_raw_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        session: Optional[Session] = None,
    ):
        _session = session or self.get_session()
        try:
            result = _session.execute(query, params)
            _session.commit()
            logger.debug("Raw query executed successfully")
            return result.fetchall()
        except SQLAlchemyError as e:
            _session.rollback()
            logger.exception("Failed to execute raw query: %s", str(e))
        finally:
            self.cleanup_session(_session if session is None else None)

    def execute_scalar(self, stmt: Executable, session: Optional[Session] = None) -> object:
        """Execute a SQL statement and return a single result or None.

        This method executes the given SQL statement and returns the result.

        Args:
            stmt (Executable): The SQLAlchemy statement to be executed.

        Returns:
            Any: The result of the executed statement.

        Raises:
            DatabaseException: If there's an error during the database operation.
        """
        _session = session or self.get_session()
        try:
            return _session.scalar(stmt)
        except (Exception, SQLAlchemyError) as e:
            logger.exception(f"Failed to execute scalar statement: {e}")
            raise DatabaseException("Unable to execute scalar statement") from e

    def scalar_one_or_none(self, stmt: Executable) -> object:
        """Execute a SQL statement and return a single result or None.

        This method executes the given SQL statement and returns either a single
        scalar result or None if no results are found.

        Args:
            stmt (Executable): The SQLAlchemy statement to be executed.

        Returns:
            Any: The scalar result of the query, or None if no results are found.

        Raises:
            DatabaseException: If there's an error during the database operation.
        """
        try:
            return self.session.execute(stmt).scalar_one_or_none()
        except (Exception, SQLAlchemyError) as e:
            logger.exception(f"Failed to get one model from database: {e}")
            raise DatabaseException("Unable to get model from database") from e

    def scalars_all(self, stmt: Executable) -> object:
        """Scalars a SQL statement and return a single result or None.

        This method executes the given SQL statement and returns the result.

        Args:
            stmt (Executable): The SQLAlchemy statement to be executed.

        Returns:
            Any: The result of the executed statement.

        Raises:
            DatabaseException: If there's an error during the database operation.
        """
        try:
            return self.session.scalars(stmt).all()
        except (Exception, SQLAlchemyError) as e:
            logger.exception(f"Failed to execute statement: {e}")
            raise DatabaseException("Unable to execute statement") from e

    def execute_all(self, stmt: Executable) -> object:
        """Execute a SQL statement and return a single result or None.

        This method executes the given SQL statement and returns the result.

        Args:
            stmt (Executable): The SQLAlchemy statement to be executed.

        Returns:
            Any: The result of the executed statement.

        Raises:
            DatabaseException: If there's an error during the database operation.
        """
        try:
            return self.session.execute(stmt).all()
        except (Exception, SQLAlchemyError) as e:
            logger.exception(f"Failed to execute statement: {e}")
            raise DatabaseException("Unable to execute statement") from e


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC)
    )
    modified_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC)
    )

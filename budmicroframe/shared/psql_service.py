from typing import Any, Dict, Generic, List, Literal, Optional, Tuple, Type, TypeVar, Union

from pydantic import PostgresDsn
from sqlalchemy import asc, create_engine, desc, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from ..commons import logging
from ..commons.config import get_app_settings, get_secrets_settings
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
                logger.info(f"Connecting to password: {password}")
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

            stmt = _session.query(self.model).insert().values(obj)
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

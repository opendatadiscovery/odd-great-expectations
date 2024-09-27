from typing import List

from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyBatchData,
    SqlAlchemyExecutionEngine,
)
from loguru import logger
from oddrn_generator import MssqlGenerator, PostgresqlGenerator, SnowflakeGenerator
from sqlalchemy import inspect
from sqlalchemy.engine import Engine


def postgres_dataset(engine: Engine, batch_data: SqlAlchemyBatchData) -> str:
    logger.info("Generating oddrn for postgres dataset...")

    inspector = inspect(batch_data.execution_engine.engine)

    ds_name = batch_data.source_table_name

    if ds_name in inspector.get_table_names():
        logger.info(f"Dataset '{ds_name}' is a table.")
        ds_path = "tables"
    elif ds_name in inspector.get_view_names():
        logger.info(f"Dataset '{ds_name}' is a view.")
        ds_path = "views"
    elif ds_name in inspector.get_materialized_view_names():
        logger.info(f"Dataset '{ds_name}' is a materialized view.")
        ds_path = "views"
    else:
        logger.info(f"Dataset '{ds_name}' is not a table, view or materialized view")
        raise Exception("Unexpected dataset type")

    generator_params = {
        "host_settings": engine.url.host,
        "databases": engine.url.database,
        "schemas": batch_data.source_schema_name or "public",
        ds_path: ds_name,
    }

    generator = PostgresqlGenerator(**generator_params)
    ds_oddrn = generator.get_oddrn_by_path(ds_path)

    logger.debug(f"Generated oddrn for '{ds_name}': {ds_oddrn}")
    return ds_oddrn


def snowflake_dataset(engine: Engine, batch_data: SqlAlchemyBatchData) -> str:
    logger.info("Generating oddrn for snowflake dataset...")

    # Snowflake might create SqlAlchemy database name like <DATABASE_NAME>/<SCHEMA_NAME>
    # TODO: create table/view type checking logic, like for postgres_dataset(), for accurate oddrn generation
    generator_params = {
        "host_settings": engine.engine.url.host,
        "databases": engine.engine.url.database.split("/")[0],
        "schemas": batch_data.source_schema_name or "public",
        "tables": batch_data.source_table_name,
    }

    generator = SnowflakeGenerator(**generator_params)
    oddrn = generator.get_oddrn_by_path("tables")
    logger.debug(f"Snowflake source {oddrn=}")
    return oddrn


def mssql_dataset(engine: Engine, batch_data: SqlAlchemyBatchData) -> str:
    logger.info("Generating oddrn for mssql dataset...")

    # TODO: create table/view type checking logic, like for postgres_dataset(), for accurate oddrn generation
    generator_params = {
        "host_settings": engine.url.host,
        "databases": engine.url.database,
        "schemas": batch_data.source_schema_name or "dbo",
        "tables": batch_data.source_table_name,
    }

    generator = MssqlGenerator(**generator_params)
    oddrn = generator.get_oddrn_by_path("tables")
    logger.debug(f"MSSQL source {oddrn=}")
    return oddrn


def get_sql_table_dataset(exec_engine: SqlAlchemyExecutionEngine) -> List[str]:
    dialect_name = exec_engine.dialect_name
    engine = exec_engine.engine
    batch_data = exec_engine.batch_manager.active_batch_data

    if dialect_name == "postgresql":
        return [postgres_dataset(engine, batch_data)]
    elif dialect_name == "snowflake":
        return [snowflake_dataset(engine, batch_data)]
    elif dialect_name == "mssql":
        return [mssql_dataset(engine, batch_data)]

    raise ValueError(f"Unknown {dialect_name=}")

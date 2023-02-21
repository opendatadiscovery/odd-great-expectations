from typing import List

from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyBatchData,
    SqlAlchemyExecutionEngine,
)
from loguru import logger
from oddrn_generator import PostgresqlGenerator, SnowflakeGenerator
from sqlalchemy.engine import Engine


def postgres_dataset(engine: Engine, batch_data: SqlAlchemyBatchData) -> str:
    generator_params = {
        "host_settings": engine.url.host,
        "databases": engine.url.database,
        "schemas": batch_data.source_schema_name or "public",
        "tables": batch_data.source_table_name,
    }

    generator = PostgresqlGenerator(**generator_params)
    return generator.get_oddrn_by_path("tables")


def snowflake_dataset(engine: Engine, batch_data: SqlAlchemyBatchData) -> str:
    # Snowflake might create SqlAlchemy database name like <DATABASE_NAME>/<SCHEMA_NAME>
    generator_params = {
        "host_settings": engine.engine.url.host,
        "databases": engine.engine.url.database.split("/")[0],
        "schemas": batch_data.source_schema_name or "public",
        "tables": batch_data.source_table_name,
    }

    generator = SnowflakeGenerator(**generator_params)
    oddrn = generator.get_oddrn_by_path("tables")
    logger.info(f"{oddrn=}")
    return oddrn


def get_sql_table_dataset(exec_engine: SqlAlchemyExecutionEngine) -> List[str]:
    dialect_name = exec_engine.dialect_name
    engine = exec_engine.engine
    batch_data = exec_engine.batch_manager.active_batch_data

    if dialect_name == "postgresql":
        return [postgres_dataset(engine, batch_data)]
    elif dialect_name == "snowflake":
        return [snowflake_dataset(engine, batch_data)]

    raise ValueError(f"Unknown {dialect_name=}")

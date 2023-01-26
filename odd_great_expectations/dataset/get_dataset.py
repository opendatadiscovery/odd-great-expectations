from typing import List

from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)

from odd_great_expectations.dataset.file import get_file_dataset
from odd_great_expectations.dataset.sql_table import get_sql_table_dataset


def get_datasets(execution_engine: ExecutionEngine) -> List[str]:
    datasets: List[str] = []
    if isinstance(execution_engine, PandasExecutionEngine):
        datasets = get_file_dataset(execution_engine)
    elif isinstance(execution_engine, SqlAlchemyExecutionEngine):
        datasets = get_sql_table_dataset(execution_engine)

    return datasets

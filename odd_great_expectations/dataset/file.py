from pathlib import Path

from great_expectations.execution_engine import PandasExecutionEngine
from loguru import logger
from oddrn_generator.generators import FilesystemGenerator


def get_file_dataset(engine: PandasExecutionEngine) -> list[str]:
    batch_spec = engine.batch_manager.active_batch.batch_spec

    logger.debug("Batch spec")
    logger.debug(batch_spec.__dict__)
    logger.debug(f"Resolved path: {Path(batch_spec['path']).resolve()}")

    batch_path = str(Path(batch_spec["path"]).resolve())
    generator = FilesystemGenerator(host_settings="local", path=batch_path)

    path = generator.get_oddrn_by_path("path")
    logger.debug(f"Resulted path: {path}")

    return [path]

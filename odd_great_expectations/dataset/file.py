from pathlib import Path

from great_expectations.execution_engine import PandasExecutionEngine
from oddrn_generator.generators import FilesystemGenerator


def get_file_dataset(engine: PandasExecutionEngine) -> list[str]:
    batch_spec = engine.batch_manager.active_batch.batch_spec
    batch_path = str(Path(batch_spec['path']).resolve())
    generator = FilesystemGenerator(host_settings='local', path=batch_path)
    return [generator.get_oddrn_by_path("path")]

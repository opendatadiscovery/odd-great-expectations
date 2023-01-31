import datetime
from typing import Any, Optional, Tuple

from great_expectations.checkpoint.actions import (
    ExpectationSuiteValidationResult, ValidationResultIdentifier)
from great_expectations.core import ExpectationConfiguration
from great_expectations.expectations.registry import get_expectation_impl
from great_expectations.validator.validator import ExpectationValidationResult
from odd_models.models import (DataEntity, DataEntityList, DataEntityType,
                               DataQualityTest, DataQualityTestExpectation,
                               DataQualityTestRun, LinkedUrl, QualityRunStatus)
from oddrn_generator.generators import GreatExpectationsGenerator


class MapValidationResult:
    def __init__(
        self,
        suite_result: ExpectationSuiteValidationResult,
        suite_result_identifier: ValidationResultIdentifier,
        generator: GreatExpectationsGenerator,
        datasets: list[str],
        docs_link: Optional[str],
    ) -> None:
        self._generator = generator
        self._datasets = datasets
        self._result = suite_result
        self._result_identifier = suite_result_identifier
        self._docs_link = docs_link

    def map(self) -> DataEntityList:
        data_entities = []
        for result in self._result.results:
            data_entities.extend(self._map_result(result))

        return DataEntityList(
            data_source_oddrn=self._generator.get_data_source_oddrn(),
            items=data_entities,
        )

    def _map_result(
        self, validation_result: ExpectationValidationResult
    ) -> tuple[DataEntity, DataEntity]:
        run_id = self._result_identifier.run_id
        status, status_reason = self.get_status(validation_result)

        job = self.map_config(validation_result.expectation_config)
        oddrn = self._generator.get_oddrn_by_path("runs", run_id.run_name)

        run = DataEntity(
            oddrn=oddrn,
            name=f"{job.name}:{run_id.run_name}",
            type=DataEntityType.JOB_RUN,
            data_quality_test_run=DataQualityTestRun(
                data_quality_test_oddrn=job.oddrn,
                start_time=run_id.run_time.replace(tzinfo=None).astimezone(),
                end_time=datetime.datetime.now().astimezone(),
                status_reason=status_reason,
                status=status,
            ),
        )
        return job, run

    def get_status(
        self, validation_result: ExpectationValidationResult
    ) -> Tuple[QualityRunStatus, str]:
        status = QualityRunStatus.SUCCESS
        status_reason = None

        if not validation_result.success:
            status = QualityRunStatus.FAILED

            if unexpected := validation_result.result.get("partial_unexpected_list"):
                status_reason = (
                    f"Unexpected values {str(unexpected)}" if unexpected else None
                )

        return status, status_reason

    def map_config(self, config: ExpectationConfiguration) -> DataEntity:
        original_type = config.expectation_type
        unique_path = uniq_name(config)

        oddrn = self._generator.get_oddrn_by_path("types", unique_path)
        suite_name = (
            self._result_identifier.expectation_suite_identifier.expectation_suite_name
        )

        linked_url_list = None
        if self._docs_link and not self._docs_link.startswith("file://"):
            linked_url_list = [LinkedUrl(name="docs", url=self._docs_link)]

        dqt = DataQualityTest(
            suite_name=suite_name,
            dataset_list=self._datasets,
            expectation=DataQualityTestExpectation(
                type=original_type, **flat_kwargs(config.kwargs)
            ),
            linked_url_list=linked_url_list,
        )

        return DataEntity(
            oddrn=oddrn,
            name=f"{suite_name}:{original_type}",
            type=DataEntityType.JOB,
            data_quality_test=dqt,
        )


def flat_kwargs(kwargs: dict[str, Any]):
    for k, v in kwargs.items():
        if isinstance(v, (list, set, tuple)):
            kwargs[k] = str(list(v))
        elif isinstance(v, dict):
            kwargs[k] = str(v)
    return kwargs


def uniq_name(config: ExpectationConfiguration):
    result = config.expectation_type
    impl = get_expectation_impl(config.expectation_type)

    result += f":{config.kwargs.get('batch_id')}"
    for key in impl.args_keys:
        if value := config.kwargs.get(key):
            if isinstance(value, (list, set, tuple)):
                value = "|".join(value)
            result += f":{key}:{value}"
    return result

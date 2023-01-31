from typing import Any, Union

from great_expectations.checkpoint.actions import (
    ExpectationSuiteValidationResult, GXCloudIdentifier, ValidationAction,
    ValidationResultIdentifier)
from great_expectations.validator.validator import Validator
from loguru import logger
from oddrn_generator.generators import GreatExpectationsGenerator

from odd_great_expectations.client import Client
from odd_great_expectations.dataset.get_dataset import get_datasets
from odd_great_expectations.mapper import MapValidationResult


class ODDAction(ValidationAction):
    def __init__(
        self,
        data_context,
        data_source_name: str,
        platform_host: str = None,
        platform_token: str = None,
    ):
        self._odd_client = Client(platform_host, platform_token)
        self._data_source_name = data_source_name
        self._host = "local"

        super().__init__(data_context)

    def _run(
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GXCloudIdentifier
        ],
        data_asset: Union[Validator, Any],
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
        payload=None,
    ):
        try:
            logger.info("Start collecting metadata")
            client = self._odd_client
            expectation_suite = data_asset.expectation_suite
            suite_name = expectation_suite.expectation_suite_name
            generator = GreatExpectationsGenerator(
                host_settings=self._host, suites=suite_name
            )

            docs_link = None
            if payload:
                if data_docs := payload.get("update_data_docs"):
                    docs_link = data_docs.get("local_site")

            client.ingest_data_source(
                data_source_oddrn=generator.get_data_source_oddrn(),
                name=self._data_source_name,
            )

            datasets = get_datasets(data_asset.execution_engine)

            data_entity_list = MapValidationResult(
                suite_result=validation_result_suite,
                suite_result_identifier=validation_result_suite_identifier,
                generator=generator,
                datasets=datasets,
                docs_link=docs_link,
            ).map()

            client.ingest_data_entities(data_entity_list)

            logger.success(
                f"Metadata successfully loaded to Platform. Ingested {len(data_entity_list.items)} entities"
            )
        except Exception as e:
            logger.error(f"Error during collecting metadata. {e}")

import traceback
from typing import Union, Optional

from great_expectations.checkpoint.actions import (
    ExpectationSuiteValidationResult,
    GXCloudIdentifier,
    ValidationAction,
    ValidationResultIdentifier,
)
from great_expectations.validator.validator import Validator
from oddrn_generator.generators import GreatExpectationsGenerator

from odd_models.api_client.v2.odd_api_client import Client
from odd_great_expectations.dataset.get_dataset import get_datasets
from odd_great_expectations.logger import logger
from odd_great_expectations.mapper import MapValidationResult


class ODDAction(ValidationAction):
    def __init__(
        self,
        data_context,
        data_source_name: str,
        platform_host: str = None,
        platform_token: str = None,
    ):
        super().__init__(data_context)

        self._odd_client = Client(platform_host, platform_token)
        self._data_source_name = data_source_name

    def _run(
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GXCloudIdentifier
        ],
        data_asset: Validator,
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
        **kwargs,
    ):
        try:
            client = self._odd_client

            expectation_suite = data_asset.expectation_suite
            suite_name = expectation_suite.expectation_suite_name
            generator = GreatExpectationsGenerator(
                host_settings="local", suites=suite_name
            )
            client.create_data_source(
                data_source_oddrn=generator.get_data_source_oddrn(),
                data_source_name=self._data_source_name,
            )

            datasets: list[str] = get_datasets(data_asset.execution_engine)
            docs_link: Optional[str] = get_docs_links(kwargs.get("payload", None))

            data_entity_list = MapValidationResult(
                suite_result=validation_result_suite,
                suite_result_identifier=validation_result_suite_identifier,
                generator=generator,
                datasets=datasets,
                docs_link=docs_link,
            ).map()

            client.ingest_data_entity_list(data_entities=data_entity_list)

            logger.success(
                f"Metadata successfully loaded to Platform. Ingested {len(data_entity_list.items)} entities"
            )
            return {"odd_action_status": "success"}
        except Exception as e:
            logger.debug(traceback.format_exc())
            logger.error(f"Error during collecting metadata. {e}")
            return {"odd_action_status": "failed"}


def get_docs_links(payload: Optional[dict]) -> Optional[str]:
    if not payload:
        return None
    for action_name in payload.keys():
        if payload[action_name]["class"] == "UpdateDataDocsAction":
            return payload[action_name].get("local_site")

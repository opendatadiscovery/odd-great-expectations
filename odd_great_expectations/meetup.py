from typing import Union

from great_expectations.checkpoint import ValidationAction
from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier, GXCloudIdentifier
from great_expectations.validator.validator import Validator
from odd_great_expectations.logger import logger

class CustomGXAction(ValidationAction):
    def __init__(self, data_context, message: str):
        super().__init__(data_context)

        self.message = message
    def _run(
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GXCloudIdentifier
        ],
        data_asset: Validator,
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
        **kwargs
    ):
        logger.info(f"Hello, {self.message}")
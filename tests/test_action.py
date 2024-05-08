import unittest
from unittest.mock import Mock, patch
from odd_great_expectations import action

class TestODDAction(unittest.TestCase):
    @patch("odd_great_expectations.action.Client")
    def setUp(self, mock_client):
        self.data_context = Mock()
        self.data_source_name = 'test_datasource'
        self.platform_host = '127.0.0.1'
        self.platform_token = '1234_token'
        self.mock_client = mock_client
        self.odd_action = action.ODDAction(self.data_context, self.data_source_name, self.platform_host, self.platform_token)

    @patch("odd_great_expectations.action.GreatExpectationsGenerator")
    @patch("odd_great_expectations.action.get_datasets")
    @patch("odd_great_expectations.action.get_docs_links")
    @patch("odd_great_expectations.action.MapValidationResult")
    @patch("odd_great_expectations.action.traceback")
    @patch("odd_great_expectations.action.logger")
    def test_run(self, mock_logger,mock_traceback, mock_map_result, mock_docs_link, mock_datasets, mock_generator):
        validation_result_suite = Mock()
        validation_result_suite_identifier = Mock()
        data_asset = Mock()

        expectation_suite = Mock()
        expectation_suite.expectation_suite_name = "Mock Suite"
        data_asset.expectation_suite = expectation_suite

        generator = mock_generator.return_value
        datasets = ['mock_dataset']
        docs_link = 'mock_link'
        map_result = mock_map_result.return_value
        map_result.map.return_value = ['mock_data']

        mock_datasets.return_value = datasets
        mock_docs_link.return_value = docs_link

        result = self.odd_action._run(validation_result_suite, validation_result_suite_identifier, data_asset)
        self.assertEqual(result, {'odd_action_status': 'failed'})
        self.mock_client.assert_called_once_with(self.platform_host, self.platform_token)
        mock_generator.assert_called_once_with(host_settings='local', suites=expectation_suite.expectation_suite_name)
        self.mock_client.return_value.create_data_source.assert_called_once()
        mock_datasets.assert_called_once_with(data_asset.execution_engine)
        mock_docs_link.assert_called_once()
        mock_map_result.assert_called_once()
        mock_map_result.return_value.map.assert_called_once()

    @patch("odd_great_expectations.action.GreatExpectationsGenerator")
    @patch("odd_great_expectations.action.get_datasets")
    @patch("odd_great_expectations.action.get_docs_links")
    @patch("odd_great_expectations.action.MapValidationResult")
    @patch("odd_great_expectations.action.traceback")
    @patch("odd_great_expectations.action.logger")
    def test_run_error(self, mock_logger,mock_traceback, mock_map_result, mock_docs_link, mock_datasets, mock_generator):
        validation_result_suite = Mock()
        validation_result_suite_identifier = Mock()
        data_asset = Mock()

        map_result = mock_map_result.return_value
        map_result.map.side_effect = Exception('Mock Exception')

        result = self.odd_action._run(validation_result_suite, validation_result_suite_identifier, data_asset)
        self.assertEqual(result, {'odd_action_status': 'failed'})

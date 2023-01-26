## OpenDataDiscovery Great Expectations metadata collecting.


## Supporting
| Feature                     | Supporting |
| --------------------------- | ---------- |
| V3 API +                    | +          |
| SqlAlchemyEngine            | +          |
| PandasEngine                | +          |
| Great Expectations V2 API - | -          |
| Cloud Solution              | -          |


`odd_great_expectation.action.ODDAction`
Is a class derived from `ValidationAction` and can be used in checkpoint actions lists.

## How to:

### Install odd-great-expectations package
```bash
pip install odd-great-expectations
```

### Add action to checkpoint:
```yaml
name: <CHECKPOINT_NAME>
config_version: 1.0
template_name:
module_name: great_expectations.checkpoint
class_name: Checkpoint
run_name_template: '%Y%m%d-%H%M%S-my-run-name-template'
expectation_suite_name:
batch_request: {}
action_list:
  # other actions
  - name: store_metadata_to_odd 
    action:
      module_name: odd_great_expectations.action
      class_name: ODDAction
      platform_host: <PLATFORM_HOST> # OpenDataDiscovery platform, i.e. http://localhost:8080
      platform_token: <PLATFORM_TOKEN> # OpenDataDiscovery token
      data_source_name: <DATA_SOURCE_NAME> # Unique name for data source, i.e. local_qa_test
evaluation_parameters: {}
```

### Run checkpoint
```bash
great_expectations checkpoint run <CHECKPOINT_NAME> 
```
### Check result
Check results on <PLATFORM_HOST> UI.



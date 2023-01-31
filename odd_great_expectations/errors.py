class CreateDataSourceError(Exception):
    def __init__(
        self, data_source_name: str, data_source_oddrn: str, message: str
    ) -> None:
        super().__init__(
            f"Couldn't register datasource {data_source_name} with oddrn {data_source_oddrn} \n"
            f"Message: {message}"
        )


class IngestionEntitiesError(Exception):
    def __init__(self, data_source_oddrn: str, message: str) -> None:
        super().__init__(
            f"Couldn't ingest entities for {data_source_oddrn=}.\n"
            f"Message: {message}"
        )

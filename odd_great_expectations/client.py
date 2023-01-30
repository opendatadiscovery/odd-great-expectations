import os

from odd_models.api_client.open_data_discovery_ingestion_api import ODDApiClient
from odd_models.models import DataEntityList, DataSource, DataSourceList


class Client:
    def __init__(self, host: str, token: str) -> None:
        host = host or os.getenv("ODD_PLATFORM_HOST", None)
        token = token or os.getenv("ODD_PLATFORM_TOKEN", None)

        if host is None:
            raise ValueError("Host was not set")

        if token is None:
            raise ValueError("Token was not set")

        self._client = ODDApiClient(host)
        self._token = token
        self._headers = {"Authorization": f"Bearer {token}"}

    def ingest_data_source(self, data_source_oddrn: str, name: str) -> None:
        response = self._client.create_data_source(
            DataSourceList(items=[DataSource(oddrn=data_source_oddrn, name=name)]),
            headers=self._headers,
        )
        response.raise_for_status()

    def ingest_data_entities(self, data_entities: DataEntityList) -> None:
        response = self._client.post_data_entity_list(
            data_entities, headers=self._headers
        )
        response.raise_for_status()

import os

import dagster as dg
import requests
from dagster_duckdb import DuckDBResource
from requests import Response


class TaxiTripResource(dg.ConfigurableResource):
    def request(self, endpoint: str) -> Response:
        return requests.get(
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/{endpoint}.parquet"
        )


class LocalFileSystemResource(dg.ConfigurableResource):
    base_path: str

    def get_conn(self):
        return os.path.abspath(self.base_path)


duckdb_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_DATABASE")  # replaced with environment variable
)

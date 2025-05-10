import os

import dagster as dg
import pandas as pd

from dagster_poc.resources import LocalFileSystemResource, TaxiTripResource


def materialize_asset(
    context: dg.AssetExecutionContext,
    taxi_resource: TaxiTripResource,
    fs_resource: LocalFileSystemResource,
) -> None:
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]  # only retrieve YYYY-MM portion
    base_path = fs_resource.get_conn()

    raw_trips = taxi_resource.request(
        endpoint=f"yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(os.path.join(base_path, f"taxi_{month_to_fetch}.parquet"), "wb") as f:
        f.write(raw_trips.content)


def materialize_metadata(
    context: dg.AssetExecutionContext,
    fs_resource: LocalFileSystemResource,
) -> dg.MaterializeResult:
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]  # only retrieve YYYY-MM portion
    base_path = fs_resource.get_conn()

    num_rows = len(
        pd.read_parquet(os.path.join(base_path, f"taxi_{month_to_fetch}.parquet"))
    )

    return dg.MaterializeResult(metadata={"dagster/row_count": num_rows})

from typing import Dict, List

import dagster as dg
import yaml

from dagster_poc import BASE_DIR
from dagster_poc.schemas.assets import (
    Asset,
    CategoricalPartitionConfig,
    DailyPartitionConfig,
    HourlyPartitionConfig,
    MonthlyPartitionConfig,
    Partition,
    WeeklyPartitionConfig,
    YamlConfiguration,
)

assets_yml = BASE_DIR / "assets" / "assets.yml"


def _load_config_from_yaml() -> YamlConfiguration:
    yaml_content = yaml.safe_load(assets_yml.read_text())
    config = YamlConfiguration.model_validate(yaml_content)

    return config


def build_dagster_partition(partition: Partition) -> dg.PartitionsDefinition:
    config = partition.config
    config_kwargs = config.model_dump(by_alias=True)

    if isinstance(config, HourlyPartitionConfig):
        return dg.HourlyPartitionsDefinition(**config_kwargs)
    if isinstance(config, DailyPartitionConfig):
        return dg.DailyPartitionsDefinition(**config_kwargs)
    if isinstance(config, WeeklyPartitionConfig):
        return dg.WeeklyPartitionsDefinition(**config_kwargs)
    if isinstance(config, MonthlyPartitionConfig):
        return dg.MonthlyPartitionsDefinition(**config_kwargs)
    if isinstance(config, CategoricalPartitionConfig):
        return dg.StaticPartitionsDefinition(**config_kwargs)
    raise ValueError("Unrecognizable configuration")


def build_dagster_asset(asset: Asset) -> dg.AssetsDefinition:
    @dg.asset(name=asset.name)
    def dg_asset():
        pass  # TODO

    return dg_asset


def build_partitions(
    partitions: List[Partition],
) -> List[Dict[str, dg.PartitionsDefinition]]:
    """Build out partitions.

    Builds out a dictionary of partition names to dagster partition definitions.
    This is so that assets can utilize the same partition definitions if needed.
    """
    definitions = []

    for pt in partitions:
        pt_def = build_dagster_partition(pt)
        definitions.append({pt.name: pt_def})

    return definitions


def build_assets(assets: List[Asset]) -> List[Dict[str, dg.AssetsDefinition]]:
    """Build out assets.

    Build out a dictionary of asset names to dagster asset definitions.
    """
    definitions = []

    for asset in assets:
        asset_def = build_dagster_asset(asset)
        definitions.append({asset.name, asset_def})

    return definitions


def build_definitions():
    config = _load_config_from_yaml()

    if config.partitions is not None:
        partition_defs = build_partitions(config.partitions)

    asset_defs = build_assets(config.assets)

    return asset_defs

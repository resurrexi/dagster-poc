import dagster as dg
import yaml

from dagster_poc import BASE_DIR
from dagster_poc.schemas.assets import AssetConfig, AssetsYamlConfig

assets_yml = BASE_DIR / "assets" / "assets.yml"


def load_config_from_yaml() -> AssetsYamlConfig:
    yaml_content = yaml.safe_load(assets_yml.read_text())
    config = AssetsYamlConfig.model_validate(yaml_content)

    return config


def build_asset(asset_config: AssetConfig) -> dg.AssetsDefinition:
    @dg.asset(name=asset_config.asset_name)
    def asset():
        pass  # TODO


def build_assets(config: AssetsYamlConfig):
    asset_definitions = []

    for asset_config in config.assets:
        definition = build_asset(asset_config)

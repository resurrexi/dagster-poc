import re
from typing import Any, List, Literal, Optional, Union

from pydantic import (
    BaseModel,
    Field,
    PositiveFloat,
    PositiveInt,
    field_validator,
    model_validator,
)
from typing_extensions import Self


class Column(BaseModel):
    name: str
    data_type: str


class MinOperator(BaseModel):
    value: PositiveInt
    operator: Literal["gt", "ge"] = "ge"


class MaxOperator(BaseModel):
    value: PositiveInt
    operator: Literal["lt", "le"] = "le"


class TimePartitionConfig(BaseModel):
    start_date: str
    end_date: Optional[str] = None
    timezone: str = "UTC"
    date_format: str = Field(default="%Y-%m-%d", serialization_alias="fmt")
    end_offset: int = 0

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def ensure_date_format(cls, value: Any) -> Any:
        if isinstance(value, str):
            if re.fullmatch(r"^\d{4}-\d{2}-\d{2}$", value) is None:
                raise ValueError("Invalid date format")
        return value


class HourlyPartitionConfig(TimePartitionConfig):
    minute_offset: int = 0


class DailyPartitionConfig(HourlyPartitionConfig):
    hour_offset: int = 0


class WeeklyPartitionConfig(DailyPartitionConfig):
    day_offset: int = 0


class MonthlyPartitionConfig(WeeklyPartitionConfig): ...


class CategoricalPartitionConfig(BaseModel):
    categories: List[str] = Field(serialization_alias="partition_keys")


class Partition(BaseModel):
    name: str
    partition_type: Literal["hourly", "daily", "weekly", "monthly", "categorical"]
    config: Union[
        HourlyPartitionConfig,
        DailyPartitionConfig,
        WeeklyPartitionConfig,
        MonthlyPartitionConfig,
        CategoricalPartitionConfig,
    ]

    @model_validator(mode="before")
    @classmethod
    def set_config(cls, data: Any) -> Any:
        if isinstance(data, dict):
            config = data.get("config", {})
            if data["partition_type"] == "hourly":
                data["config"] = HourlyPartitionConfig(**config)
            elif data["partition_type"] == "daily":
                data["config"] = DailyPartitionConfig(**config)
            elif data["partition_type"] == "weekly":
                data["config"] = WeeklyPartitionConfig(**config)
            elif data["partition_type"] == "monthly":
                data["config"] = MonthlyPartitionConfig(**config)
            elif data["partition_type"] == "categorical":
                data["config"] = CategoricalPartitionConfig(**config)
            else:
                raise ValueError("Invalid partition type")
        return data


class BaseCheckConfig(BaseModel):
    severity: Literal["warn", "error"] = "warn"


class SchemaCheckConfig(BaseCheckConfig): ...


class RowCountCheckConfig(BaseCheckConfig):
    threshold_stddev: PositiveFloat = 2.0
    min: Optional[MinOperator] = None
    max: Optional[MaxOperator] = None

    @model_validator(mode="after")
    def check_min_lt_max(self) -> Self:
        if self.min and self.max:
            if self.min.value >= self.max.value:
                raise ValueError("Min value must be less than max value")
        return self


class UniqueCheckConfig(BaseCheckConfig):
    column: str


class NumericDistributionCheckConfig(BaseCheckConfig):
    column: str


class CategoricalDistributionCheckConfig(BaseCheckConfig):
    column: str


class NullCheckConfig(BaseCheckConfig):
    column: str
    threshold_stddev: PositiveFloat = 2.0
    threshold_pct: PositiveFloat = 0.05


class Check(BaseModel):
    check_type: Literal["schema", "row_count", "unique", "n_dist", "c_dist", "nullity"]
    config: Union[
        SchemaCheckConfig,
        RowCountCheckConfig,
        UniqueCheckConfig,
        NumericDistributionCheckConfig,
        CategoricalDistributionCheckConfig,
        NullCheckConfig,
    ]

    @model_validator(mode="before")
    @classmethod
    def set_config(cls, data: Any) -> Any:
        if isinstance(data, dict):
            config = data.get("config", {})
            if data["check_type"] == "schema":
                data["config"] = SchemaCheckConfig(**config)
            elif data["check_type"] == "row_count":
                data["config"] = RowCountCheckConfig(**config)
            elif data["check_type"] == "unique":
                data["config"] = UniqueCheckConfig(**config)
            elif data["check_type"] == "n_dist":
                data["config"] = NumericDistributionCheckConfig(**config)
            elif data["check_type"] == "c_dist":
                data["config"] = CategoricalDistributionCheckConfig(**config)
            elif data["check_type"] == "nullity":
                data["config"] = NullCheckConfig(**config)
            else:
                raise ValueError("Invalid check type")
        return data


class Asset(BaseModel):
    name: str
    depends_on: Optional[str] = None
    resources: List[str]
    column_schema: Optional[List[Column]] = None
    partitions: Optional[List[str]] = None
    schedule: str = "eager"
    checks: Optional[List[Check]] = None


class YamlConfiguration(BaseModel):
    partitions: Optional[List[Partition]] = None
    assets: List[Asset]

    @field_validator("assets", "partitions")
    @classmethod
    def check_names_are_unique(cls, value: Any) -> Any:
        if value is not None and len(value) > 0:
            names = [i.name for i in value]
            if len(names) != len(set(names)):
                raise ValueError("Duplicate names found")
            return value

    @field_validator("assets")
    @classmethod
    def ensure_asset_dependencies_exist(cls, value: Any) -> Any:
        # take inventory of all asset names
        asset_names = set([asset.names for asset in value])

        # take inventory of all asset dependencies
        asset_deps = []
        for asset in value:
            if asset.depends_on is not None and len(asset.depends_on) > 0:
                asset_deps.extend(asset.depends_on)
        asset_deps = set(asset_deps)

        difference = asset_deps - asset_names
        if len(difference) > 0:
            raise ValueError("Asset has dependencies that aren't met")
        return value

    @model_validator(mode="after")
    def check_partitions_exist(self) -> Self:
        if self.partitions is not None and len(self.partitions) > 0:
            partition_names = [pt.name for pt in self.partitions]
            for asset in self.assets:
                if asset.partitions is not None:
                    checks = [pt in partition_names for pt in asset.partitions]
                    if not all(checks):
                        raise ValueError("Undefined partition names referenced")
        return self

import re
from typing import Any, List, Optional, Union

from pydantic import (
    BaseModel,
    Field,
    NonNegativeFloat,
    PositiveFloat,
    PositiveInt,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from dagster_poc.schemas.enums import (
    CheckTypeEnum,
    MaxOperatorEnum,
    MinOperatorEnum,
    PartitionTypeEnum,
    SeverityEnum,
)


class Column(BaseModel):
    name: str
    data_type: str


class MinOperator(BaseModel):
    value: int
    operator: MinOperatorEnum = MinOperatorEnum.GE


class MaxOperator(BaseModel):
    value: int
    operator: MaxOperatorEnum = MaxOperatorEnum.LE


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
                raise ValueError("Invalid `date_format`")
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
    partition_type: PartitionTypeEnum
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
            if data["partition_type"] == PartitionTypeEnum.HOURLY:
                data["config"] = HourlyPartitionConfig(**config)
            elif data["partition_type"] == PartitionTypeEnum.DAILY:
                data["config"] = DailyPartitionConfig(**config)
            elif data["partition_type"] == PartitionTypeEnum.WEEKLY:
                data["config"] = WeeklyPartitionConfig(**config)
            elif data["partition_type"] == PartitionTypeEnum.MONTHLY:
                data["config"] = MonthlyPartitionConfig(**config)
            elif data["partition_type"] == PartitionTypeEnum.CATEGORICAL:
                data["config"] = CategoricalPartitionConfig(**config)
            else:
                raise ValueError("Invalid `partition_type`")
        return data


class BaseCheckConfig(BaseModel):
    severity: SeverityEnum = SeverityEnum.WARN


class SchemaCheckConfig(BaseCheckConfig): ...


class VolumeCheckConfig(BaseCheckConfig):
    anomaly_from_n: PositiveInt = 10
    anomaly_stddev: PositiveFloat = 2.0
    min: Optional[MinOperator] = None
    max: Optional[MaxOperator] = None

    @field_validator("anomaly_from_n")
    @classmethod
    def check_anomaly_from_n_gte_3(cls, value: PositiveInt) -> PositiveInt:
        if value < 3:
            raise ValueError("Must be >= 3")
        return value

    @field_validator("min", "max")
    @classmethod
    def check_min_max_positive(cls, value: Any) -> Any:
        if value is not None and value.value <= 0:
            raise ValueError("`value` attribute must be positive")
        return value

    @model_validator(mode="after")
    def check_min_lt_max(self) -> Self:
        if self.min and self.max:
            if self.min.value >= self.max.value:
                raise ValueError("`min.value` must be less than `max.value`")
        return self


class UniqueCheckConfig(BaseCheckConfig):
    column: str


class BoundsCheckConfig(BaseCheckConfig):
    column: str
    min: Optional[MinOperator] = None
    max: Optional[MaxOperator] = None

    @model_validator(mode="after")
    def check_min_lt_max(self) -> Self:
        if self.min and self.max:
            if self.min.value >= self.max.value:
                raise ValueError("`min.value` must be less than `max.value`")
        return self


class NullCheckConfig(BaseCheckConfig):
    column: str
    threshold_pct: NonNegativeFloat = 0


class RegexCheckConfig(BaseCheckConfig):
    column: str
    pattern: str
    threshold_pct: NonNegativeFloat = 0


class Check(BaseModel):
    check_type: CheckTypeEnum
    config: Union[
        SchemaCheckConfig,
        VolumeCheckConfig,
        UniqueCheckConfig,
        BoundsCheckConfig,
        NullCheckConfig,
    ]

    @model_validator(mode="before")
    @classmethod
    def set_config(cls, data: Any) -> Any:
        if isinstance(data, dict):
            config = data.get("config", {})
            if data["check_type"] == CheckTypeEnum.SCHEMA:
                data["config"] = SchemaCheckConfig(**config)
            elif data["check_type"] == CheckTypeEnum.VOLUME:
                data["config"] = VolumeCheckConfig(**config)
            elif data["check_type"] == CheckTypeEnum.UNIQUE:
                data["config"] = UniqueCheckConfig(**config)
            elif data["check_type"] == CheckTypeEnum.BOUNDS:
                data["config"] = BoundsCheckConfig(**config)
            elif data["check_type"] == CheckTypeEnum.NULLITY:
                data["config"] = NullCheckConfig(**config)
            elif data["check_type"] == CheckTypeEnum.REGEX:
                data["config"] = RegexCheckConfig(**config)
            else:
                raise ValueError("Invalid `check_type`")
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
                raise ValueError("Duplicate `name`s found")
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
            raise ValueError("Undefined asset `name`s referenced")
        return value

    @model_validator(mode="after")
    def check_partitions_exist(self) -> Self:
        if self.partitions is not None and len(self.partitions) > 0:
            partition_names = [pt.name for pt in self.partitions]
            for asset in self.assets:
                if asset.partitions is not None:
                    checks = [pt in partition_names for pt in asset.partitions]
                    if not all(checks):
                        raise ValueError("Undefined partition `name`s referenced")
        return self

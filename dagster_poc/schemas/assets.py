from collections import Counter
from typing import List, Literal, Optional, Union

from pydantic import BaseModel, PositiveFloat, PositiveInt, model_validator
from typing_extensions import Self


class Column(BaseModel):
    name: str
    type: str


class MinOperator(BaseModel):
    value: PositiveInt
    operator: Optional[Literal["gt", "ge"]] = "ge"


class MaxOperator(BaseModel):
    value: PositiveInt
    operator: Optional[Literal["lt", "le"]] = "le"


class BasePartition(BaseModel):
    name: str
    type: Literal["hourly", "daily", "weekly", "monthly", "categorical"]


class TimePartition(BasePartition): ...


class CategoricalPartition(BasePartition):
    values: List[str]


class BaseCheck(BaseModel):
    check_type: Literal["schema", "row_count", "unique", "n_dist", "c_dist", "null"]
    severity: Literal["warn", "error"] = "warn"


class SchemaCheck(BaseCheck): ...


class RowCountCheck(BaseCheck):
    threshold_stddev: PositiveFloat = 2.0
    min: Optional["MinOperator"]
    max: Optional["MaxOperator"]

    @model_validator(mode="after")
    def check_min_lt_max(self) -> Self:
        if self.min and self.min.value and self.max and self.max.value:
            if self.min.value >= self.max.value:
                raise ValueError("Min value must be less than max value")

        return self


class UniqueCheck(BaseCheck):
    column: str


class NumericDistributionCheck(BaseCheck):
    column: str


class CategoricalDistributionCheck(BaseCheck):
    column: str


class NullCheck(BaseCheck):
    column: str
    threshold_stddev: Optional[PositiveFloat] = 2.0
    threshold_pct: Optional[PositiveFloat] = 0.05


class AssetConfig(BaseModel):
    asset_name: str
    resources: List[str]
    materialize_fn: str
    metadata_fn: str
    column_schema: Optional[List[Column]]
    partitions: Optional[Union[TimePartition, CategoricalPartition]]
    checks: Optional[List[Union[SchemaCheck, RowCountCheck]]]


class AssetsYamlConfig(BaseModel):
    assets: List[AssetConfig]

    @model_validator(mode="after")
    def check_unique_asset_names(self) -> Self:
        asset_names = [asset.asset_name for asset in self.assets]
        counts = Counter(asset_names)

        if all(value == 1 for value in counts.values()) is False:
            raise ValueError("Duplicate asset names found")

        return self

from enum import Enum


class MinOperatorEnum(str, Enum):
    GT = "gt"
    GE = "ge"


class MaxOperatorEnum(str, Enum):
    LT = "lt"
    LE = "le"


class PartitionTypeEnum(str, Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    CATEGORICAL = "categorical"


class SeverityEnum(str, Enum):
    WARN = "warn"
    ERROR = "error"


class CheckTypeEnum(str, Enum):
    SCHEMA = "schema"
    VOLUME = "volume"
    UNIQUE = "unique"
    BOUNDS = "bounds"
    NULLITY = "nullity"
    REGEX = "regex"

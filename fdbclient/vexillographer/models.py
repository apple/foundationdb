"""Data models for vexillographer."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ParamType(Enum):
    """Parameter type for options."""

    NONE = "None"
    STRING = "String"
    INT = "Int"
    BYTES = "Bytes"


class Scope(Enum):
    """Option scope types."""

    NETWORK_OPTION = "NetworkOption"
    DATABASE_OPTION = "DatabaseOption"
    TRANSACTION_OPTION = "TransactionOption"
    STREAMING_MODE = "StreamingMode"
    MUTATION_TYPE = "MutationType"
    CONFLICT_RANGE_TYPE = "ConflictRangeType"
    ERROR_PREDICATE = "ErrorPredicate"

    def get_c_description(self) -> str:
        """Get the C-style description for this scope."""
        descriptions = {
            Scope.NETWORK_OPTION: "NET_OPTION",
            Scope.DATABASE_OPTION: "DB_OPTION",
            Scope.TRANSACTION_OPTION: "TR_OPTION",
            Scope.STREAMING_MODE: "STREAMING_MODE",
            Scope.MUTATION_TYPE: "MUTATION_TYPE",
            Scope.CONFLICT_RANGE_TYPE: "CONFLICT_RANGE_TYPE",
            Scope.ERROR_PREDICATE: "ERROR_PREDICATE",
        }
        return descriptions[self]


@dataclass
class Option:
    """Represents a single option definition."""

    scope: Scope
    name: str
    code: int
    param_type: ParamType
    param_desc: Optional[str]
    comment: str
    hidden: bool = False
    persistent: bool = False
    sensitive: bool = False
    default_for: Optional[int] = None

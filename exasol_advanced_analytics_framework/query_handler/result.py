from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from exasol_data_science_utils_python.preprocessing.sql.schema.column import \
    Column


@dataclass()
class ReturnQuery:
    query: str
    query_columns: List[Column]


@dataclass()
class Result:
    pass


@dataclass()
class Continue(Result):
    query_list: List[str]
    return_query: Optional[ReturnQuery]


@dataclass()
class Finished(Result):
    final_result: Dict[str, Any]

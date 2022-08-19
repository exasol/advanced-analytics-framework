from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from exasol_data_science_utils_python.preprocessing.sql.schema.column import \
    Column


@dataclass()
class QueryHandlerReturnQuery:
    query: str
    query_columns: List[Column]


@dataclass()
class QueryHandlerResultBase:
    pass


@dataclass()
class QueryHandlerResultContinue(QueryHandlerResultBase):
    query_list: List[str]
    return_query: Optional[QueryHandlerReturnQuery]


@dataclass()
class QueryHandlerResultFinished(QueryHandlerResultBase):
    final_result: Dict[str, Any]

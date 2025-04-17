import importlib
import json
import logging
import traceback
from collections import OrderedDict
from dataclasses import (
    dataclass,
    field,
)
from enum import (
    Enum,
    auto,
)
from io import BytesIO
from typing import (
    Any,
    List,
    Optional,
    Tuple,
)

import exasol.bucketfs as bfs
import joblib

from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext
from exasol.analytics.query_handler.context.top_level_query_handler_context import (
    TopLevelQueryHandlerContext,
)
from exasol.analytics.query_handler.query.interface import Query
from exasol.analytics.query_handler.query.result.udf_query_result import UDFQueryResult
from exasol.analytics.query_handler.query.select import SelectQueryWithColumnDefinition
from exasol.analytics.query_handler.result import (
    Continue,
    Finish,
    Result,
)
from exasol.analytics.query_handler.udf.connection_lookup import UDFConnectionLookup
from exasol.analytics.query_handler.udf.interface import UDFQueryHandlerFactory
from exasol.analytics.query_handler.udf.runner.state import QueryHandlerRunnerState
from exasol.analytics.schema import (
    Column,
    SchemaName,
    UDFNameBuilder,
)
from exasol.analytics.utils.errors import UninitializedAttributeError


def create_bucketfs_location_from_conn_object(bfs_conn_obj) -> bfs.path.PathLike:
    bfs_params = json.loads(bfs_conn_obj.address)
    bfs_params.update(json.loads(bfs_conn_obj.user))
    bfs_params.update(json.loads(bfs_conn_obj.password))
    return bfs.path.build_path(**bfs_params)


def upload_via_joblib(location: bfs.path.PathLike, object: Any):
    buffer = BytesIO()
    joblib.dump(object, buffer)
    location.write(buffer.getvalue())


def read_via_joblib(location: bfs.path.PathLike) -> Any:
    buffer = BytesIO()
    for chunk in location.read():
        buffer.write(chunk)
    return joblib.load(buffer)


@dataclass
class UDFParameter:
    iter_num: int
    temporary_bfs_location_conn: str
    temporary_bfs_location_directory: str
    temporary_name_prefix: str
    #
    # The following attributes are only needed in the first iteration and
    # hence need to be Optional, see method
    # QueryHandlerRunnerUDF._get_parameter().
    #
    temporary_schema_name: Optional[str] = None
    python_class_name: Optional[str] = None
    python_class_module: Optional[str] = None
    parameter: Optional[str] = None


class QueryHandlerStatus(Enum):
    CONTINUE = auto()
    FINISHED = auto()
    ERROR = auto()


@dataclass
class UDFResult:
    input_query_view: Optional[str] = None
    input_query: Optional[str] = None
    final_result: str = "{}"
    query_list: List[Query] = field(default_factory=list)
    cleanup_query_list: List[Query] = field(default_factory=list)
    status: QueryHandlerStatus = QueryHandlerStatus.CONTINUE


class QueryHandlerRunnerUDF:

    def __init__(self, exa):
        self.exa = exa
        self._bucketfs_location = None
        self._parameter = None

    @property
    def _checked_bucketfs_location(self) -> bfs.path.PathLike:
        if self._bucketfs_location is None:
            raise UninitializedAttributeError("BucketFS location is undefined.")
        return self._bucketfs_location

    @property
    def _checked_parameter(self) -> UDFParameter:
        if self._parameter is None:
            raise UninitializedAttributeError("Parameter is undefined.")
        return self._parameter

    def run(self, ctx) -> None:
        self._parameter = self._get_parameter(ctx)
        self._bucketfs_location = self._create_bucketfs_location()
        current_state = self._create_state_or_load_latest_state()
        try:
            if self._checked_parameter.iter_num == 0:
                query_handler_result = current_state.query_handler.start()
            else:
                query_result = self._create_udf_query_result(
                    ctx, current_state.input_query_output_columns
                )
                query_handler_result = current_state.query_handler.handle_query_result(
                    query_result
                )

            udf_result = self.handle_query_handler_result(
                query_handler_result, current_state
            )
            if isinstance(query_handler_result, Continue):
                self._save_current_state(current_state)
            if self._checked_parameter.iter_num > 0:
                self._remove_previous_state()
            self.emit_udf_result(ctx, udf_result)
        except Exception as e:
            self.handle_exception(ctx, current_state)

    def handle_exception(self, ctx, current_state: QueryHandlerRunnerState):
        stacktrace = traceback.format_exc()
        logging.exception("Caught exception, starting cleanup.")
        try:
            self.release_query_handler_context(current_state)
        except:
            logging.exception(
                "Caught exception during handling cleanup of another exception"
            )
        cleanup_queries = (
            current_state.top_level_query_handler_context.cleanup_released_object_proxies()
        )
        udf_result = UDFResult()
        udf_result.cleanup_query_list = cleanup_queries
        udf_result.final_result = stacktrace
        udf_result.status = QueryHandlerStatus.ERROR
        self.emit_udf_result(ctx, udf_result)

    def handle_query_handler_result(
        self, query_handler_result: Result, current_state: QueryHandlerRunnerState
    ) -> UDFResult:
        if isinstance(query_handler_result, Finish):
            udf_result = self.handle_query_handler_result_finished(
                current_state, query_handler_result
            )
        elif isinstance(query_handler_result, Continue):
            udf_result = self.handle_query_handler_result_continue(
                current_state, query_handler_result
            )
        else:
            raise RuntimeError(f"Unknown query_handler_result {query_handler_result}")
        udf_result.cleanup_query_list = (
            current_state.top_level_query_handler_context.cleanup_released_object_proxies()
        )
        return udf_result

    def handle_query_handler_result_finished(
        self, current_state: QueryHandlerRunnerState, query_handler_result: Finish
    ) -> UDFResult:
        udf_result = UDFResult()
        udf_result.final_result = query_handler_result.result
        udf_result.status = QueryHandlerStatus.FINISHED
        self.release_query_handler_context(current_state)
        return udf_result

    @staticmethod
    def release_query_handler_context(current_state: QueryHandlerRunnerState):
        if current_state.input_query_query_handler_context is not None:
            current_state.input_query_query_handler_context.release()
        current_state.top_level_query_handler_context.release()

    def handle_query_handler_result_continue(
        self, current_state: QueryHandlerRunnerState, query_handler_result: Continue
    ) -> UDFResult:
        udf_result = UDFResult()
        udf_result.status = QueryHandlerStatus.CONTINUE
        udf_result.query_list = query_handler_result.query_list
        current_state.input_query_output_columns = (
            query_handler_result.input_query.output_columns
        )
        self.release_and_create_query_handler_context_if_input_query(current_state)
        if current_state.input_query_query_handler_context is None:
            raise UninitializedAttributeError(
                "Current state has no input query handler context."
            )
        udf_result.input_query_view, udf_result.input_query = self._wrap_return_query(
            current_state.input_query_query_handler_context,
            query_handler_result.input_query,
        )
        return udf_result

    @staticmethod
    def release_and_create_query_handler_context_if_input_query(
        current_state: QueryHandlerRunnerState,
    ):
        if current_state.input_query_query_handler_context is not None:
            current_state.input_query_query_handler_context.release()
        current_state.input_query_query_handler_context = (
            current_state.top_level_query_handler_context.get_child_query_handler_context()
        )

    def _get_parameter(self, ctx) -> UDFParameter:
        iter_num = ctx[0]
        if iter_num == 0:
            return UDFParameter(
                iter_num=iter_num,
                temporary_bfs_location_conn=ctx[1],
                temporary_bfs_location_directory=ctx[2],
                temporary_name_prefix=ctx[3],
                temporary_schema_name=ctx[4],
                python_class_name=ctx[5],
                python_class_module=ctx[6],
                parameter=ctx[7],
            )
        return UDFParameter(
            iter_num=iter_num,
            temporary_bfs_location_conn=ctx[1],
            temporary_bfs_location_directory=ctx[2],
            temporary_name_prefix=ctx[3],
        )

    def _create_bucketfs_location(self) -> bfs.path.PathLike:
        bfscon = self.exa.get_connection(
            self._checked_parameter.temporary_bfs_location_conn
        )
        bfs_location = create_bucketfs_location_from_conn_object(bfscon)
        return bfs_location.joinpath(
            self._checked_parameter.temporary_bfs_location_directory
        ).joinpath(self._checked_parameter.temporary_name_prefix)

    def _create_state_or_load_latest_state(self) -> QueryHandlerRunnerState:
        if self._checked_parameter.iter_num > 0:
            query_handler_state = self._load_latest_state()
        else:
            query_handler_state = self._create_state()
        return query_handler_state

    @property
    def _query_handler_factory(self) -> UDFQueryHandlerFactory:
        module_name = self._checked_parameter.python_class_module
        if not module_name:
            raise ValueError("UDFQueryHandler parameters must define a python module")
        module = importlib.import_module(module_name)
        class_name = self._checked_parameter.python_class_name
        if not class_name:
            raise ValueError("UDFQueryHandler parameters must define a factory class")
        factory = getattr(module, class_name)
        if not factory:
            raise ValueError(
                f'class "{class_name}" not found in module "{module_name}"'
            )
        return factory()

    def _create_state(self) -> QueryHandlerRunnerState:
        connection_lookup = UDFConnectionLookup(self.exa)
        if self._checked_parameter.temporary_schema_name is None:
            raise UninitializedAttributeError("Temporary schema name is undefined.")
        context = TopLevelQueryHandlerContext(
            self._checked_bucketfs_location,
            self._checked_parameter.temporary_name_prefix,
            self._checked_parameter.temporary_schema_name,
            connection_lookup,
        )
        str_parameter = self._checked_parameter.parameter or ""
        query_handler_obj = self._query_handler_factory.create(
            str_parameter,
            context,
        )
        query_handler_state = QueryHandlerRunnerState(
            top_level_query_handler_context=context,
            query_handler=query_handler_obj,
            connection_lookup=connection_lookup,
        )
        return query_handler_state

    def _load_latest_state(self) -> QueryHandlerRunnerState:
        path = self._state_file_bucketfs_location()
        state = read_via_joblib(path)
        state.connection_lookup.exa = self.exa
        return state

    def _save_current_state(self, current_state: QueryHandlerRunnerState) -> None:
        path = self._state_file_bucketfs_location(1)
        upload_via_joblib(path, current_state)

    def _remove_previous_state(self) -> None:
        self._state_file_bucketfs_location().rm()

    def _create_udf_query_result(
        self, ctx, query_columns: Optional[List[Column]]
    ) -> UDFQueryResult:
        colum_start_ix = 8 if self._checked_parameter.iter_num == 0 else 4
        if query_columns is None:
            raise UninitializedAttributeError("Query Columns is not initialized.")
        column_mapping = OrderedDict(
            [
                (str(colum_start_ix + index), column.name.name)
                for index, column in enumerate(query_columns)
            ]
        )
        return UDFQueryResult(ctx, self.exa, column_mapping=column_mapping)

    def _wrap_return_query(
        self,
        query_handler_context: ScopeQueryHandlerContext,
        input_query: SelectQueryWithColumnDefinition,
    ) -> Tuple[str, str]:
        temporary_view_name = query_handler_context.get_temporary_view_name()
        query_handler_udf_name = UDFNameBuilder.create(
            name=self.exa.meta.script_name,
            schema=SchemaName(self.exa.meta.script_schema),
        )
        query_create_view = f"CREATE VIEW {temporary_view_name.fully_qualified} AS {input_query.query_string};"
        full_qualified_columns = [
            col.name.fully_qualified for col in input_query.output_columns
        ]
        call_columns = [
            f"{self._checked_parameter.iter_num + 1}",
            f"'{self._checked_parameter.temporary_bfs_location_conn}'",
            f"'{self._checked_parameter.temporary_bfs_location_directory}'",
            f"'{self._checked_parameter.temporary_name_prefix}'",
        ]
        columns_str = ",".join(call_columns + full_qualified_columns)
        query_query_handler = (
            f"SELECT {query_handler_udf_name.fully_qualified}({columns_str}) "
            f"FROM {temporary_view_name.fully_qualified};"
        )
        return query_create_view, query_query_handler

    def _get_query_columns(self) -> List[Column]:
        query_columns = []
        for i in range(len(self.exa.meta.input_columns)):
            col_name = self.exa.meta.input_columns[i].name
            col_type = self.exa.meta.input_columns[i].sql_type
            query_columns.append(Column.from_name_and_sql_type(col_name, col_type))
        return query_columns

    def _state_file_bucketfs_location(self, iter_offset: int = 0) -> bfs.path.PathLike:
        num_iter = self._checked_parameter.iter_num + iter_offset
        return self._checked_bucketfs_location / f"state/{str(num_iter)}.pkl"

    @staticmethod
    def emit_udf_result(ctx, udf_result: UDFResult):
        ctx.emit(udf_result.input_query_view)
        ctx.emit(udf_result.input_query)
        ctx.emit(str(udf_result.status.name))
        ctx.emit(str(udf_result.final_result))
        for query in udf_result.cleanup_query_list:
            ctx.emit(query.query_string)
        for query in udf_result.query_list:
            ctx.emit(query.query_string)

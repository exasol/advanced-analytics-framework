import dataclasses
import importlib
import logging
import traceback
from collections import OrderedDict
from enum import Enum, auto
from pathlib import PurePosixPath
from typing import Tuple, List, Optional

from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation
from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory
from exasol_data_science_utils_python.schema.column import \
    Column
from exasol_data_science_utils_python.schema.column_name \
    import ColumnName
from exasol_data_science_utils_python.schema.column_type \
    import ColumnType
from exasol_data_science_utils_python.schema.schema_name \
    import SchemaName
from exasol_data_science_utils_python.schema.udf_name_builder import UDFNameBuilder

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.context.top_level_query_handler_context import \
    TopLevelQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.query.select_query import SelectQueryWithColumnDefinition
from exasol_advanced_analytics_framework.query_handler.result \
    import Finish, Continue, Result
from exasol_advanced_analytics_framework.query_result.udf_query_result \
    import UDFQueryResult
from exasol_advanced_analytics_framework.udf_framework.query_handler_runner_state \
    import QueryHandlerRunnerState


@dataclasses.dataclass
class UDFParameter:
    iter_num: int
    temporary_bfs_location_conn: str
    temporary_bfs_location_directory: str
    temporary_name_prefix: str
    temporary_schema_name: Optional[str] = None
    python_class_name: Optional[str] = None
    python_class_module: Optional[str] = None
    parameters: Optional[str] = None


class QueryHandlerStatus(Enum):
    CONTINUE = auto()
    FINISHED = auto()
    ERROR = auto()


@dataclasses.dataclass
class UDFResult:
    input_query_view: Optional[str] = None
    input_query: Optional[str] = None
    final_result = {}
    query_list = []
    cleanup_query_list = []
    status: QueryHandlerStatus = QueryHandlerStatus.CONTINUE


class QueryHandlerRunnerUDF:

    def __init__(self, exa):
        self.exa = exa
        self.bucketfs_location: Optional[AbstractBucketFSLocation] = None
        self.parameter: Optional[UDFParameter] = None

    def run(self, ctx) -> None:
        self._get_parameter(ctx)
        self._create_bucketfs_location()
        current_state = self._create_state_or_load_latest_state()
        try:
            if self.parameter.iter_num == 0:
                query_handler_result = current_state.query_handler.start()
            else:
                query_result = self._create_udf_query_result(ctx, current_state.input_query_output_columns)
                query_handler_result = current_state.query_handler.handle_query_result(query_result)

            udf_result = self.handle_query_handler_result(query_handler_result, current_state)
            if isinstance(query_handler_result, Continue):
                self._save_current_state(current_state)
            if self.parameter.iter_num > 0:
                self._remove_previous_state()
            self.emit_udf_result(ctx, udf_result)
        except Exception as e:
            self.handle_exception(ctx, current_state)

    def handle_exception(self, ctx,
                         current_state: QueryHandlerRunnerState):
        stacktrace = traceback.format_exc()
        logging.exception("Catched exception, starting cleanup.")
        try:
            self.release_query_handler_context(current_state)
        except:
            logging.exception("Catched exception during handling cleanup of another exception")
        cleanup_queries = current_state.top_level_query_handler_context.cleanup_released_object_proxies()
        udf_result = UDFResult()
        udf_result.cleanup_query_list = cleanup_queries
        udf_result.final_result = stacktrace
        udf_result.status = QueryHandlerStatus.ERROR
        self.emit_udf_result(ctx, udf_result)

    def handle_query_handler_result(self,
                                    query_handler_result: Result,
                                    current_state: QueryHandlerRunnerState):
        if isinstance(query_handler_result, Finish):
            udf_result = self.handle_query_handler_result_finished(current_state, query_handler_result)
        elif isinstance(query_handler_result, Continue):
            udf_result = self.handle_query_handler_result_continue(current_state, query_handler_result)
        else:
            raise RuntimeError(f"Unknown query_handler_result {query_handler_result}")
        udf_result.cleanup_query_list = \
            current_state.top_level_query_handler_context.cleanup_released_object_proxies()
        return udf_result

    def handle_query_handler_result_finished(
            self,
            current_state: QueryHandlerRunnerState,
            query_handler_result: Finish):
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

    def handle_query_handler_result_continue(self,
                                             current_state: QueryHandlerRunnerState,
                                             query_handler_result: Continue):
        udf_result = UDFResult()
        udf_result.status = QueryHandlerStatus.CONTINUE
        udf_result.query_list = query_handler_result.query_list
        current_state.input_query_output_columns = query_handler_result.input_query.output_columns
        self.release_and_create_new_return_query_query_handler_context(current_state)
        udf_result.input_query_view, udf_result.input_query = \
            self._wrap_return_query(current_state.input_query_query_handler_context,
                                    query_handler_result.input_query)
        return udf_result

    @staticmethod
    def release_and_create_new_return_query_query_handler_context(current_state: QueryHandlerRunnerState):
        if current_state.input_query_query_handler_context is not None:
            current_state.input_query_query_handler_context.release()
        current_state.input_query_query_handler_context = \
            current_state.top_level_query_handler_context.get_child_query_handler_context()

    def _get_parameter(self, ctx):
        iter_num = ctx[0]
        if iter_num == 0:
            self.parameter = UDFParameter(
                iter_num=iter_num,
                temporary_bfs_location_conn=ctx[1],
                temporary_bfs_location_directory=ctx[2],
                temporary_name_prefix=ctx[3],
                temporary_schema_name=ctx[4],
                python_class_name=ctx[5],
                python_class_module=ctx[6],
                parameters=ctx[7])
        else:
            self.parameter = UDFParameter(
                iter_num=iter_num,
                temporary_bfs_location_conn=ctx[1],
                temporary_bfs_location_directory=ctx[2],
                temporary_name_prefix=ctx[3])

    def _create_bucketfs_location(self):
        bucketfs_connection_obj = self.exa.get_connection(self.parameter.temporary_bfs_location_conn)
        bucketfs_location_from_con = BucketFSFactory().create_bucketfs_location(
            url=bucketfs_connection_obj.address,
            user=bucketfs_connection_obj.user,
            pwd=bucketfs_connection_obj.password)
        self.bucketfs_location = bucketfs_location_from_con \
            .joinpath(self.parameter.temporary_bfs_location_directory) \
            .joinpath(self.parameter.temporary_name_prefix)

    def _create_state_or_load_latest_state(self) -> QueryHandlerRunnerState:
        if self.parameter.iter_num > 0:
            query_handler_state = self._load_latest_state()
        else:
            query_handler_state = self._create_state()
        return query_handler_state

    def _create_state(self):
        context = TopLevelQueryHandlerContext(self.bucketfs_location,
                                              self.parameter.temporary_name_prefix,
                                              self.parameter.temporary_schema_name)
        module = importlib.import_module(self.parameter.python_class_module)
        query_handler_class = getattr(module, self.parameter.python_class_name)
        query_handler_obj = query_handler_class(self.parameter.parameters, context)
        query_handler_state = QueryHandlerRunnerState(
            top_level_query_handler_context=context,
            query_handler=query_handler_obj)
        return query_handler_state

    def _load_latest_state(self):
        state_file_bucketfs_path = self._generate_state_file_bucketfs_path()
        query_handler_state = self.bucketfs_location.read_file_from_bucketfs_via_joblib(str(state_file_bucketfs_path))
        return query_handler_state

    def _save_current_state(self, current_state: QueryHandlerRunnerState) -> None:
        next_state_file_bucketfs_path = self._generate_state_file_bucketfs_path(1)
        self.bucketfs_location.upload_object_to_bucketfs_via_joblib(
            current_state, str(next_state_file_bucketfs_path))

    def _remove_previous_state(self) -> None:
        state_file_bucketfs_path = self._generate_state_file_bucketfs_path()
        self.bucketfs_location.delete_file_in_bucketfs(str(state_file_bucketfs_path))

    def _create_udf_query_result(
            self, ctx, query_columns: List[Column]) -> UDFQueryResult:
        colum_start_ix = 8 if self.parameter.iter_num == 0 else 4
        column_mapping = OrderedDict([
            (str(colum_start_ix + index), column.name.name)
            for index, column in enumerate(query_columns)])
        return UDFQueryResult(ctx, self.exa, column_mapping=column_mapping)

    def _wrap_return_query(self,
                           query_handler_context: ScopeQueryHandlerContext,
                           input_query: SelectQueryWithColumnDefinition) \
            -> Tuple[str, str]:
        temporary_view = query_handler_context.get_temporary_view_name()
        query_handler_udf_name = \
            UDFNameBuilder.create(
                name="AAF_QUERY_HANDLER_UDF",
                schema=SchemaName(self.exa.meta.script_schema)
            )
        query_create_view = \
            f"CREATE VIEW {temporary_view.fully_qualified()} AS {input_query.query_string};"
        full_qualified_columns = [col.name.fully_qualified()
                                  for col in input_query.output_columns]
        call_columns = [
            f"{self.parameter.iter_num + 1}",
            f"'{self.parameter.temporary_bfs_location_conn}'",
            f"'{self.parameter.temporary_bfs_location_directory}'",
            f"'{self.parameter.temporary_name_prefix}'",
        ]
        columns_str = ",".join(call_columns + full_qualified_columns)
        query_query_handler = \
            f"SELECT {query_handler_udf_name.fully_qualified()}({columns_str}) " \
            f"FROM {temporary_view.fully_qualified()};"
        return query_create_view, query_query_handler

    def _get_query_columns(self):
        query_columns: List[Column] = []
        for i in range(len(self.exa.meta.input_columns)):
            col_name = self.exa.meta.input_columns[i].name
            col_type = self.exa.meta.input_columns[i].sql_type
            query_columns.append(
                Column(ColumnName(col_name), ColumnType(col_type)))
        return query_columns

    def _generate_state_file_bucketfs_path(self, iter_offset: int = 0) -> PurePosixPath:
        num_iter = self.parameter.iter_num + iter_offset
        return PurePosixPath(f"state/{str(num_iter)}.pkl")

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

import dataclasses
import importlib
from collections import OrderedDict
from pathlib import PurePosixPath
from typing import Tuple, List, Optional

from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation
from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory
from exasol_data_science_utils_python.preprocessing.sql.schema.column import \
    Column
from exasol_data_science_utils_python.preprocessing.sql.schema.column_name \
    import ColumnName
from exasol_data_science_utils_python.preprocessing.sql.schema.column_type \
    import ColumnType
from exasol_data_science_utils_python.preprocessing.sql.schema.schema_name \
    import SchemaName
from exasol_data_science_utils_python.preprocessing.sql.schema.table_name \
    import TableName

from exasol_advanced_analytics_framework.event_context.udf_event_context \
    import UDFEventContext
from exasol_advanced_analytics_framework.event_handler.context.top_level_event_handler_context import \
    TopLevelEventHandlerContext
from exasol_advanced_analytics_framework.event_handler.event_handler_result \
    import EventHandlerReturnQuery, EventHandlerResultFinished, EventHandlerResultContinue, EventHandlerResultBase
from exasol_advanced_analytics_framework.event_handler.event_handler_state \
    import EventHandlerState


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


@dataclasses.dataclass
class UDFResult:
    return_query_view: Optional[str] = None
    return_query: Optional[str] = None
    final_result = {}
    query_list = []
    is_finished = False


class CreateEventHandlerUDF:

    def __init__(self, exa):
        self.exa = exa
        self.bucketfs_location: Optional[AbstractBucketFSLocation] = None
        self.parameter: Optional[UDFParameter] = None

    def run(self, ctx) -> None:
        self._get_parameter(ctx)
        self._create_bucketfs_location()
        current_state = self._create_state_or_load_latest_state()
        udf_event_context = self._create_udf_event_context(ctx, current_state.query_columns)

        event_handler_result = current_state.event_handler.handle_event(
            udf_event_context, current_state.event_handler_context)

        udf_result = self.create_udf_result(event_handler_result)
        if isinstance(event_handler_result, EventHandlerResultContinue):
            self._save_current_state(current_state, event_handler_result.return_query)
        if self.parameter.iter_num > 0:
            self._remove_previous_state()
        self.emit_udf_result(ctx, udf_result)

    def create_udf_result(self, event_handler_result: EventHandlerResultBase):
        udf_result = UDFResult()
        if isinstance(event_handler_result, EventHandlerResultFinished):
            udf_result.final_result = event_handler_result.final_result
            udf_result.is_finished = True
        elif isinstance(event_handler_result, EventHandlerResultContinue):
            udf_result.is_finished = False
            udf_result.query_list = event_handler_result.query_list
            udf_result.return_query_view, udf_result.return_query = \
                self._wrap_return_query(event_handler_result.return_query)
        return udf_result

    def emit_udf_result(self, ctx, udf_result: UDFResult):
        ctx.emit(udf_result.return_query_view)
        ctx.emit(udf_result.return_query)
        ctx.emit(str(udf_result.is_finished))
        ctx.emit(str(udf_result.final_result))
        for query in udf_result.query_list:
            ctx.emit(query)

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
            .joinpath(self.parameter.temporary_bfs_location_directory)\
            .joinpath(self.parameter.temporary_name_prefix)

    def _create_state_or_load_latest_state(self) -> EventHandlerState:
        if self.parameter.iter_num > 0:
            event_handler_state = self._load_latest_state()
        else:
            event_handler_state = self._create_state()
        return event_handler_state

    def _create_state(self):
        context = TopLevelEventHandlerContext(self.bucketfs_location,
                                              self.parameter.temporary_name_prefix,
                                              self.parameter.temporary_schema_name)
        module = importlib.import_module(self.parameter.python_class_module)
        event_handler_class = getattr(module, self.parameter.python_class_name)
        event_handler_obj = event_handler_class(self.parameter.parameters)
        event_handler_state = EventHandlerState(
            context, event_handler_obj, self._get_query_columns())
        return event_handler_state

    def _load_latest_state(self):
        state_file_bucketfs_path = self._generate_state_file_bucketfs_path()
        event_handler_state = self.bucketfs_location.read_file_from_bucketfs_via_joblib(str(state_file_bucketfs_path))
        return event_handler_state

    def _save_current_state(
            self,
            current_state: EventHandlerState,
            return_query: EventHandlerReturnQuery) -> None:
        next_state_file_bucketfs_path = self._generate_state_file_bucketfs_path(1)
        current_state = EventHandlerState(
            current_state.event_handler_context,
            current_state.event_handler,
            return_query.query_columns)
        self.bucketfs_location.upload_object_to_bucketfs_via_joblib(
            current_state, str(next_state_file_bucketfs_path))

    def _remove_previous_state(self) -> None:
        state_file_bucketfs_path = self._generate_state_file_bucketfs_path()
        self.bucketfs_location.delete_file_in_bucketfs(str(state_file_bucketfs_path))

    def _create_udf_event_context(
            self, ctx, query_columns: List[Column]) -> UDFEventContext:
        colum_start_ix = 5 if self.parameter.iter_num == 0 else 3
        column_mapping = OrderedDict([
            (str(colum_start_ix + index), column.name.fully_qualified())
            for index, column in enumerate(query_columns)])
        return UDFEventContext(ctx, self.exa, column_mapping=column_mapping)

    def _wrap_return_query(self, return_query: EventHandlerReturnQuery) \
            -> Tuple[str, str]:
        tmp_view_name = TableName(
            table_name="TMP_VIEW",
            schema=SchemaName(self.parameter.temporary_schema_name)).fully_qualified()
        # TODO don't misuse TableName
        event_handler_udf_name = TableName(
            table_name="AAF_EVENT_HANDLER_UDF",
            schema=SchemaName(self.exa.meta.script_schema)).fully_qualified()
        query_create_view = \
            f"Create view {tmp_view_name} as {return_query.query};"
        full_qualified_columns = [col.name.fully_qualified()
                                  for col in return_query.query_columns]
        call_columns = [
            f"{self.parameter.iter_num + 1}",
            f"'{self.parameter.temporary_bfs_location_conn}'",
            f"'{self.parameter.temporary_bfs_location_directory}'",
            f"'{self.parameter.temporary_name_prefix}'",
        ]
        columns_str = ",".join(call_columns + full_qualified_columns)
        print(columns_str)
        query_event_handler = \
            f"SELECT {event_handler_udf_name}({columns_str}) " \
            f"FROM {tmp_view_name};"
        return query_create_view, query_event_handler

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

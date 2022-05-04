import importlib
from collections import OrderedDict
from pathlib import PurePosixPath
from typing import Tuple, List
from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory
from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation
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
from exasol_advanced_analytics_framework.event_handler.event_handler_base \
    import EventHandlerBase
from exasol_advanced_analytics_framework.event_handler.event_handler_context \
    import EventHandlerContext
from exasol_advanced_analytics_framework.event_handler.event_handler_result \
    import EventHandlerReturnQuery, EventHandlerResultBase, \
    EventHandlerResultFinished
from exasol_advanced_analytics_framework.event_handler.event_handler_state \
    import EventHandlerState
from exasol_advanced_analytics_framework.event_context.udf_event_context \
    import UDFEventContext


class CreateEventHandlerUDF:

    def __init__(self, exa):
        self.exa = exa
        self.bucketfs_location = None

    def run(self, ctx) -> None:
        # get and set method parameters
        iter_num = ctx[0]  # iter_num
        bucketfs_connection = ctx[1]  # bucketfs_connection_name
        event_handler_class = ctx[2]  # event_handler_class_name
        bucketfs_connection_obj = self.exa.get_connection(bucketfs_connection)

        self.bucketfs_location = BucketFSFactory().create_bucketfs_location(
            url=bucketfs_connection_obj.address,
            user=bucketfs_connection_obj.user,
            pwd=bucketfs_connection_obj.password
        )

        # load the latest (create if not) event handler state object
        latest_state = self._load_latest_state(
            ctx, iter_num, event_handler_class)
        event_handler_context: EventHandlerContext = latest_state.context
        event_handler: EventHandlerBase = latest_state.event_handler
        query_columns: List[Column] = latest_state.query_columns

        # call the user code
        udf_event_context = self._create_udf_event_context(
            ctx, iter_num, query_columns)
        result: EventHandlerResultBase = event_handler.handle_event(
            udf_event_context, event_handler_context)

        # handle state transition
        return_query_view = None
        return_query = None
        final_result = {}
        query_list = []
        if isinstance(result, EventHandlerResultFinished):
            final_result = result.final_result
        else:
            query_list = result.query_list

            # save current state
            self._save_current_state(
                iter_num + 1, event_handler_class,
                event_handler_context, event_handler, result.return_query)

            # wrap return query
            return_query_view, return_query = self._wrap_return_query(
                iter_num,
                bucketfs_connection,
                self.exa.meta.script_schema,
                result.return_query)

        # remove previous state
        self._remove_previous_state(
            iter_num, event_handler_class)

        # emits
        ctx.emit(return_query_view)
        ctx.emit(return_query)
        ctx.emit(str(result.is_finished))
        ctx.emit(final_result)
        for query in query_list:
            ctx.emit(query)

    def _load_latest_state(
            self,
            ctx,
            iter_num: int,
            event_handler_class: str) -> EventHandlerState:

        bucketfs_path = self._generate_bucketfs_path(
            iter_num, event_handler_class)
        if iter_num > 0:
            # load the latest state
            event_handler_state = self.bucketfs_location.\
                read_file_from_bucketfs_via_joblib(str(bucketfs_path))
        else:
            # create the new state
            event_handler_module = ctx[3]
            parameters = ctx[4]

            context = EventHandlerContext(
                self.bucketfs_location, bucketfs_path)
            event_handler_class = getattr(importlib.import_module(
                event_handler_module), event_handler_class)
            event_handler_obj = event_handler_class(parameters)
            event_handler_state = EventHandlerState(
                context, event_handler_obj, self._get_query_columns())

        return event_handler_state

    def _save_current_state(
            self,
            iter_num: int,
            event_handler_class: str,
            event_handler_context: EventHandlerContext,
            event_handler: EventHandlerBase,
            return_query: EventHandlerReturnQuery) -> None:

        current_bucketfs_path = self._generate_bucketfs_path(
            iter_num, event_handler_class)
        current_state = EventHandlerState(
            event_handler_context, event_handler,
            return_query.query_columns)
        self.__save_state(
            current_state, current_bucketfs_path, self.bucketfs_location)

    def _remove_previous_state(
            self,
            iter_num: int,
            event_handler_class: str) -> None:
        bucketfs_path = self._generate_bucketfs_path(
            iter_num, event_handler_class)
        self.__remove_state(bucketfs_path, self.bucketfs_location)

    def _create_udf_event_context(
            self, ctx, iter_num: int,
            query_columns: List[Column]) -> UDFEventContext:
        colum_start_ix = 5 if iter_num == 0 else 3
        column_mapping = OrderedDict([
            (str(colum_start_ix + index), column.name.fully_qualified())
            for index, column in enumerate(query_columns)])
        return UDFEventContext(ctx, self.exa, column_mapping=column_mapping)

    @staticmethod
    def __remove_state(
            bucketfs_path: PurePosixPath,
            bucketfs_location: BucketFSLocation) -> None:
        bucketfs_location.delete_file_in_bucketfs(
            str(bucketfs_path))

    @staticmethod
    def __save_state(
            current_state: EventHandlerState,
            bucketfs_path: PurePosixPath,
            bucketfs_location: BucketFSLocation) -> None:
        bucketfs_location.upload_object_to_bucketfs_via_joblib(
            current_state, str(bucketfs_path))

    @staticmethod
    def _wrap_return_query(
            iter_num: int, bucketfs_conn: str, schema: str,
            return_query: EventHandlerReturnQuery) \
            -> Tuple[str, str]:
        tmp_view_name = TableName(
            table_name="TMP_VIEW",
            schema=SchemaName(schema)).fully_qualified()
        event_handler_udf_name = TableName(
            table_name="AAF_EVENT_HANDLER_UDF",
            schema=SchemaName(schema)).fully_qualified()
        query_create_view = \
            f"Create view {tmp_view_name} as {return_query.query};"
        columns_str = \
            ",".join([col.name.fully_qualified()
                      for col in return_query.query_columns])
        query_event_handler = \
            f"SELECT {event_handler_udf_name}" \
            f"({iter_num},'{bucketfs_conn}',{columns_str}) " \
            f"FROM {tmp_view_name};"
        return query_create_view, query_event_handler

    def _get_query_columns(self):
        query_columns: List[Column] = []
        for i in range(self.exa.meta.input_column_count):
            col_name = self.exa.meta.input_columns[i].name
            col_type = self.exa.meta.input_columns[i].sql_type
            query_columns.append(
                Column(ColumnName(col_name), ColumnType(col_type)))
        return query_columns

    @staticmethod
    def _generate_bucketfs_path(
            iter_num: int, event_handler_class: str) -> PurePosixPath:
        return PurePosixPath(f"{event_handler_class}_{str(iter_num)}.pkl")

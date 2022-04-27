import importlib
from collections import OrderedDict
from pathlib import PurePosixPath
from typing import Tuple

from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory
from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation
from exasol_advanced_analytics_framework.event_handler.event_handler_base \
    import EventHandlerBase
from exasol_advanced_analytics_framework.event_handler.event_handler_context \
    import EventHandlerContext
from exasol_advanced_analytics_framework.event_handler.event_handler_result \
    import EventHandlerResult
from exasol_advanced_analytics_framework.event_handler.event_handler_state \
    import EventHandlerState
from exasol_advanced_analytics_framework.context_wrapper.udf_context_wrapper \
    import UDFContextWrapper


class CreateEventHandlerUDF:

    def __init__(self, exa):
        self.exa = exa

    def run(self, ctx) -> None:
        # get and set method parameters
        iter_num = ctx[0]  # iter_num
        event_handler_module = ctx[1]  # event_handler_module
        event_handler_class = ctx[2]  # event_handler_class_name
        bucketfs_connection = ctx[3]  # bucketfs_connection_name
        parameters = ctx[4]  # event_handler_parameters

        bucketfs_location = BucketFSFactory().create_bucketfs_location(
            url=bucketfs_connection.address,
            user=bucketfs_connection.user,
            pwd=bucketfs_connection.password,
            base_path=parameters["base"]
        )
        latest_bucketfs_path = PurePosixPath(
            f"{event_handler_class}_{str(iter_num)}.pkl")

        # load the latest (create if not) event handler state object
        latest_state = self._load_latest_state(
            iter_num, event_handler_class, event_handler_module,
            bucketfs_location, latest_bucketfs_path)
        event_handler_context: EventHandlerContext = latest_state.context
        event_handler: EventHandlerBase = latest_state.event_handler

        # call the user code
        udf_context = self._create_udf_context_wrapper(ctx)
        result: EventHandlerResult = event_handler.handle_event(
            udf_context, event_handler_context)

        # save the current state
        iter_num += 1
        current_bucketfs_path = PurePosixPath(
            f"{event_handler_class}_{str(iter_num)}.pkl")
        current_state = EventHandlerState(event_handler_context, event_handler)
        self._save_current_state(
            bucketfs_location, current_bucketfs_path, current_state)

        # remove previous state
        self._remove_previous_state(
            bucketfs_location, latest_bucketfs_path)

        # wrap return query
        return_query_view, return_query = self._wrap_return_query(
            iter_num, bucketfs_connection,
            result.return_query, result.return_query_columns)

        # return queries
        ctx.emit(return_query_view)
        ctx.emit(return_query)
        ctx.emit(result.status)
        for query in result.query_list:
            ctx.emit(query)

    @staticmethod
    def _load_latest_state(
            iter_num: int,
            event_handler_module: str,
            event_handler_class: str,
            bucketfs_location: BucketFSLocation,
            bucketfs_path: PurePosixPath) -> EventHandlerState:

        if iter_num > 0:
            # load the latest state
            event_handler_state = bucketfs_location.\
                download_object_from_bucketfs_via_joblib(str(bucketfs_path))
        else:
            # create the new state
            context = EventHandlerContext(
                bucketfs_location, bucketfs_path)
            event_handler_class = getattr(importlib.import_module(
                event_handler_module), event_handler_class)
            event_handler_obj = event_handler_class()
            event_handler_state = EventHandlerState(context, event_handler_obj)

        return event_handler_state

    @staticmethod
    def _remove_previous_state(
            bucketfs_location: BucketFSLocation,
            bucketfs_path: PurePosixPath) -> None:
        # TODO-2: requests.delete(url, ..)
        pass

    @staticmethod
    def _save_current_state(
            bucketfs_location: BucketFSLocation,
            bucketfs_path: PurePosixPath,
            current_state: EventHandlerState) -> None:
        bucketfs_location.upload_object_to_bucketfs_via_joblib(
            current_state, str(bucketfs_path))

    @staticmethod
    def _create_udf_context_wrapper(ctx) -> UDFContextWrapper:
        # TODO: need a simplified UDFContextWrapper ?
        df = ctx.get_dataframe(1)
        column_name_list = df["2"][0].split(",")
        column_mapping = OrderedDict([
            (str(4 + index), column)
            for index, column in enumerate(column_name_list)])
        return UDFContextWrapper(ctx, column_mapping=column_mapping)

    @staticmethod
    def _wrap_return_query(
            iter_num: int, bucketfs_conn: str,
            query: str, query_columns: dict) -> Tuple[str,str]:
        columns_str = ",".join(query_columns.keys())
        tmp_view_name = "tmp_view".upper()
        event_handler_udf_name = "AAF_EVENT_HANDLER_UDF".upper()
        query_create_view = f"Create view {tmp_view_name} as {query};"
        query_event_handler = f"SELECT {event_handler_udf_name}" \
                              f"({iter_num},'{bucketfs_conn}',{columns_str}) " \
                              f"FROM {tmp_view_name};"
        return query_create_view, query_event_handler

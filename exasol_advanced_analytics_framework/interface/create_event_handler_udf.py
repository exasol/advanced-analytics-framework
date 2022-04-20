import importlib
from collections import OrderedDict
from pathlib import PurePosixPath
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
        parameters = ctx.parameters
        bucketfs_connection = ctx.bucketfs_connection
        bucketfs_location = BucketFSFactory().create_bucketfs_location(
            url=bucketfs_connection.address,
            user=bucketfs_connection.user,
            pwd=bucketfs_connection.password,
            base_path=parameters["base"]
        )
        bucketfs_path = PurePosixPath(
            f"{parameters['bucket_file_path']}.pkl")

        # load the latest (create if not) event handler state object
        latest_state = self._load_latest_state(
            parameters, bucketfs_location, bucketfs_path)
        event_handler_context: EventHandlerContext = latest_state.context
        event_handler: EventHandlerBase = latest_state.event_handler

        # call the user code
        udf_context = self._create_udf_context_wrapper(ctx)
        result: EventHandlerResult = event_handler.handle_event(
            udf_context, event_handler_context)  # TODO does user change state?

        # update and save (overwrite)the current state
        current_state = EventHandlerState(event_handler_context, event_handler)
        self._save_current_state(
            bucketfs_location, bucketfs_path, current_state)

        # return queries
        ctx.emit(result.return_query)
        ctx.emit(result.status)
        for query in result.query_list:
            ctx.emit(query)

    def _load_latest_state(
            self,
            parameters: dict,
            bucketfs_location: BucketFSLocation,
            bucketfs_path: PurePosixPath) -> EventHandlerState:

        event_handler_state = bucketfs_location.\
            download_object_from_bucketfs_via_joblib(str(bucketfs_path))

        if not event_handler_state:
            context = EventHandlerContext(
                bucketfs_location, bucketfs_path)
            event_handler_class = getattr(importlib.import_module(
                parameters["module_name"]), parameters["class_name"])
            event_handler_obj = event_handler_class()
            event_handler_state = EventHandlerState(context, event_handler_obj)

        return event_handler_state

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

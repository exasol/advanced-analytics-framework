from exasol_advanced_analytics_framework.event_handler.event_handler_base import \
    EventHandlerBase
from exasol_advanced_analytics_framework.event_handler.event_handler_context import EventHandlerContext
import importlib

from exasol_advanced_analytics_framework.event_handler.event_handler_result import \
    EventHandlerResult


class CreateEventHandlerUDF:
    def __init__(self, exa):
        self.exa = exa

    def run(self, ctx) -> None:
        parameters = ctx.parameters
        bucketfs_connection = ctx.bucketfs_connection

        event__handler_context = EventHandlerContext(bucketfs_connection)
        event_handler_obj = self._get_event_handler_obj(
            parameters,  event__handler_context)
        result: EventHandlerResult = event_handler_obj.handle_event()  # TODO ?
        self._save_event_handler_obj(event_handler_obj, event__handler_context)

        ctx.emit(result.return_query)
        ctx.emit(result.status)
        for query in result.query_list:
            ctx.emit(query)

    def _get_event_handler_obj(
            self, parameters, event__handler_context) -> EventHandlerBase:

        event_handler_obj = event__handler_context.\
            temporary_bucketfs_file_manager.load_class(parameters['name'])
        if not event_handler_obj:
            event_handler_class = MyClass = getattr(importlib.import_module(
                parameters["module_name"]), parameters["class_name"])
            event_handler_obj = event_handler_class()

        return event_handler_obj

    def _save_event_handler_obj(
            self, event_handler_obj, event__handler_context) -> None:
        event__handler_context.\
            temporary_bucketfs_file_manager.save_class(event_handler_obj)

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import (
    Callable,
    Generic,
    Iterator,
    Optional,
    TypeAlias,
    TypeVar,
    Union,
    cast,
)

from pydantic import BaseModel

from exasol.analytics.audit.audit import AuditTable
from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_graph import SQLStageGraph
from exasol.analytics.query_handler.query.interface import Query
from exasol.analytics.query_handler.query.result.interface import QueryResult
from exasol.analytics.query_handler.query.select import (
    AuditQuery,
    SelectQueryWithColumnDefinition,
)
from exasol.analytics.query_handler.query_handler import QueryHandler
from exasol.analytics.query_handler.result import (
    Continue,
    Finish,
)


class Phase(Enum):
    MAIN = 2
    RUN_FINAL_AUDIT_QUERY = 3
    FINISHED = 4


ResultType = TypeVar("ResultType")
ParameterType = TypeVar("ParameterType")
ContinueOrFinish: TypeAlias = Union[Continue, Finish[ResultType]]


class IllegalMethodCallError(RuntimeError):
    """
    The application framework logic called a method that was not intended
    to be called, e.g. ``IdentityQueryHandler.handle_query_result()``.
    """


@dataclass
class Child(Generic[ResultType]):
    context: ScopeQueryHandlerContext
    query_handler: QueryHandler
    result: ResultType | None


class AuditQueryHandler(QueryHandler[ParameterType, ResultType]):
    """
    Use the specified factory (e.g. :class:`OrchestratorQueryHandler`) to
    instantiate a query handler and augment the queries returned by it for
    creating Audit Log entries.
    """

    def __init__(
        self,
        parameter: ParameterType,
        context: ScopeQueryHandlerContext,
        query_handler_factory: Callable[
            [ParameterType, ScopeQueryHandlerContext],
            QueryHandler[ParameterType, ResultType],
        ],
    ):
        super().__init__(parameter, context)
        self._phase = Phase.MAIN
        self._audit_table: AuditTable | None = None

        child_context = context.get_child_query_handler_context()
        query_handler = query_handler_factory(parameter, child_context)
        self._child = Child[ResultType](child_context, query_handler, None)

    def start(self) -> ContinueOrFinish:
        action = self._child.query_handler.start()
        return self._handle_action(action)

    def handle_query_result(self, query_result: QueryResult) -> ContinueOrFinish:
        if self._phase == Phase.MAIN:
            action = self._child.query_handler.handle_query_result(query_result)
            return self._handle_action(action)
        elif self._phase == Phase.RUN_FINAL_AUDIT_QUERY:
            self._phase = Phase.FINISHED
            return Finish(result=self._child.result, audit_query=None)
        else:
            raise IllegalMethodCallError(
                f"Method {type(self).__name__}.handle_query_result()"
                f" has been called in phase {self._phase.name}."
            )

    def _handle_action(self, action: ContinueOrFinish) -> ContinueOrFinish:
        if isinstance(action, Continue):
            caction = cast(Continue, action)
            return Continue(
                query_list=self._augmented(caction.query_list),
                input_query=caction.input_query,
            )
        elif isinstance(action, Finish):
            self._child.context.release()
            if action.audit_query:
                self._child.result = action.result
                self._phase = Phase.RUN_FINAL_AUDIT_QUERY
                return self._final_continue(action.audit_query)
            else:
                self._phase = Phase.FINISHED
                return action
        else:
            raise RuntimeError(f"Unknown action type {type(action)}")

    def _final_continue(self, audit_query: AuditQuery) -> Continue:
        """
        Create Continue for excecuting the final AuditQuery.
        """
        input_query = SelectQueryWithColumnDefinition(
            query_string="SELECT 1",
            output_columns=[],
        )
        return Continue(
            query_list=self._augmented([audit_query]),
            input_query=input_query,
        )

    def _augmented(self, queries: list[Query]) -> list[Query]:
        def generate() -> Iterator[Query]:
            if self._audit_table is None:
                self._audit_table = AuditTable(
                    db_schema="<TBD>",
                    table_name_prefix="<TBD>",
                    additional_columns=[],  # TBD
                )
                yield self._audit_table.create_query
            yield from self._audit_table.augment(iter(queries))

        return list(generate())

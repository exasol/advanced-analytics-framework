import dataclasses
from test.utils.mock_cast import mock_cast
from typing import Union
from unittest.mock import (
    MagicMock,
    call,
    create_autospec,
)

import pytest

from exasol.analytics.query_handler.context.proxy.object_proxy import ObjectProxy
from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext
from exasol.analytics.query_handler.graph.stage.sql.execution.object_proxy_reference_counter import (
    ObjectProxyReferenceCounter,
    ReferenceCounterStatus,
)

MockScopeQueryHandlerContext = Union[ScopeQueryHandlerContext, MagicMock]
MockObjectProxy = Union[ObjectProxy, MagicMock]


@dataclasses.dataclass
class TestMockSetup:
    __test__ = False
    mock_parent_query_context_handler: MockScopeQueryHandlerContext
    mock_child_query_context_handler: MockScopeQueryHandlerContext
    mock_object_proxy: MockObjectProxy

    def reset_mock(self):
        self.mock_parent_query_context_handler.reset_mock()
        self.mock_child_query_context_handler.reset_mock()
        self.mock_child_query_context_handler.reset_mock()


def create_test_setup() -> TestMockSetup:
    parent_query_context_handler: MockScopeQueryHandlerContext = create_autospec(
        ScopeQueryHandlerContext
    )
    child_query_context_handler: MockScopeQueryHandlerContext = create_autospec(
        ScopeQueryHandlerContext
    )
    mock_cast(
        parent_query_context_handler.get_child_query_handler_context
    ).side_effect = [child_query_context_handler]
    object_proxy: MockObjectProxy = create_autospec(ObjectProxy)
    return TestMockSetup(
        mock_parent_query_context_handler=parent_query_context_handler,
        mock_child_query_context_handler=child_query_context_handler,
        mock_object_proxy=object_proxy,
    )


def test_init():
    """
    This test checks the creation of the ObjectProxyReferenceCounter.
    It expects that get_child_query_handler_context is called on the parent_query_context_handler
    and that the object_proxy is transfer from the parent_query_context_handler to the
    child_query_context_handler.
    """
    test_setup = create_test_setup()
    counter = ObjectProxyReferenceCounter(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxy
    )
    mock_cast(
        test_setup.mock_parent_query_context_handler.get_child_query_handler_context
    ).assert_called_once()
    mock_cast(
        test_setup.mock_parent_query_context_handler.transfer_object_to
    ).assert_called_once_with(
        test_setup.mock_object_proxy, test_setup.mock_child_query_context_handler
    )


def test_single_add():
    """
    This test checks a single call to add.
    It expects no calls to the parent_query_context_handler and child_query_handler_context.
    """
    test_setup = create_test_setup()
    counter = ObjectProxyReferenceCounter(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxy
    )
    test_setup.reset_mock()
    counter.add()
    assert (
        test_setup.mock_parent_query_context_handler.mock_calls == []
        and test_setup.mock_child_query_context_handler.mock_calls == []
    )


def test_single_add_and_single_remove():
    """
    This test checks a single call to add, followed by a single call to remove.
    It expects no calls to the parent_query_context_handler and child_query_handler_context
    and that the remove returns NOT_RELEASED.
    """
    test_setup = create_test_setup()
    counter = ObjectProxyReferenceCounter(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxy
    )
    counter.add()
    test_setup.reset_mock()
    reference_counter_status = counter.remove()
    assert (
        reference_counter_status == ReferenceCounterStatus.NOT_RELEASED
        and test_setup.mock_parent_query_context_handler.mock_calls == []
        and test_setup.mock_child_query_context_handler.mock_calls == []
    )


def test_single_add_and_two_removes():
    """
    This test checks a single call to add, followed by two calls to remove.
    It expects a call to release on the child_query_context_handler.
    and that the remove returns RELEASED.
    """
    test_setup = create_test_setup()
    counter = ObjectProxyReferenceCounter(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxy
    )
    counter.add()
    counter.remove()
    test_setup.reset_mock()
    reference_counter_status = counter.remove()
    assert (
        reference_counter_status == ReferenceCounterStatus.RELEASED
        and test_setup.mock_child_query_context_handler.mock_calls == [call.release()]
        and test_setup.mock_parent_query_context_handler.mock_calls == []
    )


def test_single_remove():
    """
    This test calls remove() after ObjectProxyReferenceCounter creation.
    It expects the return value ReferenceCounterStatus.RELEASED
    and that mock_child_query_context_handler.release is called
    """
    test_setup = create_test_setup()
    counter = ObjectProxyReferenceCounter(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxy
    )
    test_setup.reset_mock()
    reference_counter_status = counter.remove()
    assert (
        reference_counter_status == ReferenceCounterStatus.RELEASED
        and test_setup.mock_child_query_context_handler.mock_calls == [call.release()]
        and test_setup.mock_parent_query_context_handler.mock_calls == []
    )


def test_add_after_release():
    """
    This test checks that we fail, when we call add after we already released the counter.
    """
    test_setup = create_test_setup()
    counter = ObjectProxyReferenceCounter(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxy
    )
    test_setup.reset_mock()
    reference_counter_status = counter.remove()
    assert reference_counter_status == ReferenceCounterStatus.RELEASED
    with pytest.raises(
        RuntimeError,
        match="ReferenceCounter not valid anymore. "
        "ObjectProxy got already garbage collected or transfered back.",
    ):
        counter.add()


def test_remove_after_release():
    """
    This test checks that we fail, when we call remove after we already released the counter.
    """
    test_setup = create_test_setup()
    counter = ObjectProxyReferenceCounter(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxy
    )
    test_setup.reset_mock()
    reference_counter_status = counter.remove()
    assert reference_counter_status == ReferenceCounterStatus.RELEASED
    with pytest.raises(
        RuntimeError,
        match="ReferenceCounter not valid anymore. "
        "ObjectProxy got already garbage collected or transfered back.",
    ):
        counter.remove()


@pytest.mark.parametrize("count", list(range(2, 10)))
def test_multiple_adds_and_removes_after_each_other(count: int):
    """
    This test calls first a series of adds followed by the same number of removes on the ObjectProxyReferenceCounter
    and a final time remove. It expects:
        - that the first removes return ReferenceCounterStatus.NOT_RELEASED
        - that the last remove returns ReferenceCounterStatus.RELEASED
        - that release gets called on the child query handler context
    """
    test_setup = create_test_setup()
    counter = ObjectProxyReferenceCounter(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxy
    )
    test_setup.reset_mock()
    for i in range(count):
        counter.add()
    reference_counter_status_of_first_removes = [counter.remove() for i in range(count)]
    last_reference_counter_status = counter.remove()
    assert (
        last_reference_counter_status == ReferenceCounterStatus.RELEASED
        and test_setup.mock_child_query_context_handler.mock_calls == [call.release()]
        and test_setup.mock_parent_query_context_handler.mock_calls == []
        and reference_counter_status_of_first_removes
        == [ReferenceCounterStatus.NOT_RELEASED] * count
    )


@pytest.mark.parametrize("count", list(range(2, 10)))
def test_multiple_adds_and_removes_after_alternating(count: int):
    """
    This test calls add and remove alternating on the ObjectProxyReferenceCounter and a final time
    remove. It expects:
        - that the first removes return ReferenceCounterStatus.NOT_RELEASED
        - that the last remove returns ReferenceCounterStatus.RELEASED
        - that release gets called on the child query handler context
    """
    test_setup = create_test_setup()
    counter = ObjectProxyReferenceCounter(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxy
    )
    test_setup.reset_mock()

    reference_counter_status_of_first_removes = []
    for i in range(count):
        counter.add()
        reference_counter_status_of_first_removes.append(counter.remove())

    last_reference_counter_status = counter.remove()
    assert (
        last_reference_counter_status == ReferenceCounterStatus.RELEASED
        and test_setup.mock_child_query_context_handler.mock_calls == [call.release()]
        and test_setup.mock_parent_query_context_handler.mock_calls == []
        and reference_counter_status_of_first_removes
        == [ReferenceCounterStatus.NOT_RELEASED] * count
    )


def test_transfer_back_to_parent_query_handler_context_after_init():
    """
    This test if we can transfer the object proxy from the child query context handler
    back to the parent query context handler after we created the ObjectProxyReferenceCounter.
    It expects that transfer_object_to is called on the child query context handler with the object proxy
    and the parent query context handler, followed by a release call.
    """
    test_setup = create_test_setup()
    counter = ObjectProxyReferenceCounter(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxy
    )
    test_setup.reset_mock()
    counter.transfer_back_to_parent_query_handler_context()
    assert test_setup.mock_parent_query_context_handler.mock_calls == []
    test_setup.mock_child_query_context_handler.assert_has_calls(
        [
            call.transfer_object_to(
                test_setup.mock_object_proxy,
                test_setup.mock_parent_query_context_handler,
            ),
            call.release(),
        ]
    )


def test_transfer_back_to_parent_query_handler_context_after_add():
    """
    This test if you can transfer_back_to_parent_query_handler_context the object proxy
    back after add was called. It expects the same as for a transfer after we created the
    ObjectProxyReferenceCounter.
    """
    test_setup = create_test_setup()
    counter = ObjectProxyReferenceCounter(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxy
    )
    counter.add()
    test_setup.reset_mock()
    counter.transfer_back_to_parent_query_handler_context()
    assert test_setup.mock_parent_query_context_handler.mock_calls == []
    test_setup.mock_child_query_context_handler.assert_has_calls(
        [
            call.transfer_object_to(
                test_setup.mock_object_proxy,
                test_setup.mock_parent_query_context_handler,
            ),
            call.release(),
        ]
    )


def test_transfer_back_to_parent_query_handler_context_after_release():
    """
    This test that a call to transfer_back_to_parent_query_handler_context fails,
    when we already have called remove after ObjectProxyReferenceCounter creation,
    which lead to the release of the ObjectProxy.
    """
    test_setup = create_test_setup()
    counter = ObjectProxyReferenceCounter(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxy
    )
    counter.remove()
    test_setup.reset_mock()
    with pytest.raises(
        RuntimeError,
        match="ReferenceCounter not valid anymore. "
        "ObjectProxy got already garbage collected or transfered back.",
    ):
        counter.transfer_back_to_parent_query_handler_context()


def test_two_transfer_back_to_parent_query_handler_context():
    """
    This test if a second call to transfer_back_to_parent_query_handler_context
    fails after a first successful transfer_back_to_parent_query_handler_context.
    """
    test_setup = create_test_setup()
    counter = ObjectProxyReferenceCounter(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxy
    )
    counter.transfer_back_to_parent_query_handler_context()
    test_setup.reset_mock()
    with pytest.raises(
        RuntimeError,
        match="ReferenceCounter not valid anymore. "
        "ObjectProxy got already garbage collected or transfered back.",
    ):
        counter.transfer_back_to_parent_query_handler_context()


def test_remove_after_transfer_back_to_parent_query_handler_context():
    """
    This test if a remove after a call to transfer_back_to_parent_query_handler_context fails.
    """
    test_setup = create_test_setup()
    counter = ObjectProxyReferenceCounter(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxy
    )
    counter.transfer_back_to_parent_query_handler_context()
    test_setup.reset_mock()
    with pytest.raises(
        RuntimeError,
        match="ReferenceCounter not valid anymore. "
        "ObjectProxy got already garbage collected or transfered back.",
    ):
        counter.remove()


def test_add_after_transfer_back_to_parent_query_handler_context():
    """
    This test if a add after a call to transfer_back_to_parent_query_handler_context fails.
    """
    test_setup = create_test_setup()
    counter = ObjectProxyReferenceCounter(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxy
    )
    counter.transfer_back_to_parent_query_handler_context()
    test_setup.reset_mock()
    with pytest.raises(
        RuntimeError,
        match="ReferenceCounter not valid anymore. "
        "ObjectProxy got already garbage collected or transfered back.",
    ):
        counter.add()

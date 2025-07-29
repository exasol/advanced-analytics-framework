import dataclasses
from typing import (
    List,
    Union,
)
from unittest.mock import (
    MagicMock,
    Mock,
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
from exasol.analytics.query_handler.graph.stage.sql.execution.object_proxy_reference_counting_bag import (
    ObjectProxyReferenceCounterFactory,
    ObjectProxyReferenceCountingBag,
)
from tests.utils.mock_cast import mock_cast

MockObjectProxyReferenceCounter = Union[ObjectProxyReferenceCounter, MagicMock]
MockObjectProxyReferenceCounterFactory = Union[ObjectProxyReferenceCounterFactory, Mock]
MockScopeQueryHandlerContext = Union[ScopeQueryHandlerContext, MagicMock]
MockObjectProxy = Union[ObjectProxy, MagicMock]


@dataclasses.dataclass
class TestSetup:
    __test__ = False
    mock_parent_query_context_handler: MockScopeQueryHandlerContext
    mock_object_proxy_reference_counter_factory: MockObjectProxyReferenceCounterFactory
    mock_object_proxies: list[MockObjectProxy]
    mock_object_proxy_reference_counters: list[MockObjectProxyReferenceCounter]

    def reset_mock(self):
        self.mock_parent_query_context_handler.reset_mock()
        self.mock_object_proxy_reference_counter_factory.reset_mock()
        for proxy in self.mock_object_proxies:
            proxy.reset_mock()
        for counter in self.mock_object_proxy_reference_counters:
            counter.reset_mock()


def create_test_setup(*, proxy_count: int) -> TestSetup:
    parent_query_context_handler: MockScopeQueryHandlerContext = create_autospec(
        ScopeQueryHandlerContext
    )
    object_proxies: list[MockObjectProxy] = [
        create_autospec(ObjectProxy) for i in range(proxy_count)
    ]
    object_proxy_reference_counter_factory: MockObjectProxyReferenceCounterFactory = (
        Mock()
    )
    object_proxy_reference_counters = create_test_setup_with_reference_counters(
        object_proxies
    )
    object_proxy_reference_counter_factory.side_effect = object_proxy_reference_counters
    return TestSetup(
        parent_query_context_handler,
        object_proxy_reference_counter_factory,
        object_proxies,
        object_proxy_reference_counters,
    )


def create_test_setup_with_reference_counters(
    mock_object_proxies: list[MockObjectProxy],
):
    object_proxy_reference_counters = [
        create_mock_reference_counter() for _ in mock_object_proxies
    ]
    return object_proxy_reference_counters


def create_mock_reference_counter() -> MockObjectProxyReferenceCounter:
    object_proxy_reference_counter: MockObjectProxyReferenceCounter = create_autospec(
        ObjectProxyReferenceCounter
    )

    @dataclasses.dataclass
    class Counter:
        count: int = 1

        def remove(self):
            self.count -= 1
            if self.count == 0:
                return ReferenceCounterStatus.RELEASED
            elif self.count > 0:
                return ReferenceCounterStatus.NOT_RELEASED
            else:
                raise AssertionError("Counter must be larger then 0")

        def add(self):
            self.count += 1

    counter = Counter()
    mock_cast(object_proxy_reference_counter.remove).side_effect = counter.remove
    mock_cast(object_proxy_reference_counter.add).side_effect = counter.add
    return object_proxy_reference_counter


def test_init():
    test_setup = create_test_setup(proxy_count=1)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    test_setup.mock_parent_query_context_handler.assert_not_called()
    test_setup.mock_object_proxy_reference_counter_factory.assert_not_called()
    assert test_setup.mock_object_proxy_reference_counters[0].mock_calls == []


def test_single_object_proxy_add():
    """
    This test adds a object proxy mock to the ObjectProxyReferenceCountingBag and
    that a ObjectProxyReferenceCounter is created for it.
    """
    test_setup = create_test_setup(proxy_count=1)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    test_setup.mock_object_proxy_reference_counter_factory.assert_called_once_with(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxies[0]
    )
    assert test_setup.mock_parent_query_context_handler.mock_calls == []
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[0].remove
    ).assert_not_called()


def test_single_object_proxy_add_contains():
    """
    This test adds a object proxy mock to the ReferenceCountingBag and
    expects that __contains__ returns true for this object proxy mock
    """
    test_setup = create_test_setup(proxy_count=1)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    assert test_setup.mock_object_proxies[0] in bag


def test_single_object_proxy_not_added_contains():
    """
    This test checks if a object proxy mock is not contained in the bag when not added.
    """
    test_setup = create_test_setup(proxy_count=1)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    assert test_setup.mock_object_proxies[0] not in bag


def test_single_object_proxy_add_remove_contains():
    """
    This test adds a object proxy mock to the ObjectProxyReferenceCountingBag and
    removes it directly after it. The test expects that __contains__ returns false
    for this object proxy mock.
    """
    test_setup = create_test_setup(proxy_count=1)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    bag.remove(test_setup.mock_object_proxies[0])
    assert test_setup.mock_object_proxies[0] not in bag


def test_multiple_object_proxy_add_contains():
    """
    This test adds two object proxy mocks to the ObjectProxyReferenceCountingBag and
    expects that __contains__ returns true for these object proxy mocks
    """
    test_setup = create_test_setup(proxy_count=2)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    bag.add(test_setup.mock_object_proxies[1])
    assert (
        test_setup.mock_object_proxies[0] in bag
        and test_setup.mock_object_proxies[1] in bag
    )


def test_single_object_proxy_add_remove():
    """
    This test adds and removes a object proxy mock to/from the ObjectProxyReferenceCountingBag and
    expects that the remove method of the ObjectProxyReferenceCounter is called
    """
    test_setup = create_test_setup(proxy_count=1)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    test_setup.reset_mock()
    bag.remove(test_setup.mock_object_proxies[0])
    assert test_setup.mock_parent_query_context_handler.mock_calls == []
    test_setup.mock_object_proxy_reference_counter_factory.assert_not_called()
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[0].remove
    ).assert_called_once()
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[0].add
    ).assert_not_called()


def test_single_object_proxy_add_add():
    """
    This test adds a object proxy mock twice to the ObjectProxyReferenceCountingBag and
    expects besides the behavior for the first add, no further interactions with the mocks.
    """
    test_setup = create_test_setup(proxy_count=1)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    test_setup.reset_mock()
    bag.add(test_setup.mock_object_proxies[0])
    assert test_setup.mock_parent_query_context_handler.mock_calls == []
    test_setup.mock_object_proxy_reference_counter_factory.assert_not_called()
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[0].add
    ).assert_called_once()
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[0].remove
    ).assert_not_called()


def test_single_object_proxy_add_add_remove():
    """
    This test adds a object proxy mock twice to the ObjectProxyReferenceCountingBag and then removes it once.
    It expects the behavior for the first add and a call to remove of the ObjectProxyRefereneCounter
    """
    test_setup = create_test_setup(proxy_count=1)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    bag.add(test_setup.mock_object_proxies[0])
    test_setup.reset_mock()
    bag.remove(test_setup.mock_object_proxies[0])
    assert test_setup.mock_parent_query_context_handler.mock_calls == []
    test_setup.mock_object_proxy_reference_counter_factory.assert_not_called()
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[0].remove
    ).assert_called_once()
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[0].add
    ).assert_not_called()


def test_single_object_proxy_add_add_remove_remove():
    """
    This test adds a object proxy mock twice to the ObjectProxyReferenceCountingBag and then removes it twice.
    Besides behavior of the adds and the first remove, we expect a call to the remove of ObjectProxyReferenceCounter
    """
    test_setup = create_test_setup(proxy_count=1)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    bag.add(test_setup.mock_object_proxies[0])
    bag.remove(test_setup.mock_object_proxies[0])
    test_setup.reset_mock()
    bag.remove(test_setup.mock_object_proxies[0])
    assert test_setup.mock_parent_query_context_handler.mock_calls == []
    test_setup.mock_object_proxy_reference_counter_factory.assert_not_called()
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[0].remove
    ).assert_called_once()
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[0].add
    ).assert_not_called()


def test_multiple_object_proxies_add():
    """
    This test adds multiple object proxy mocks to the ObjectProxyReferenceCountingBag.
    It expects the create or two ObjectProxyReferenceCounter with the factory.
    """
    test_setup = create_test_setup(proxy_count=2)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    bag.add(test_setup.mock_object_proxies[1])
    test_setup.mock_object_proxy_reference_counter_factory.assert_has_calls(
        [
            call(
                test_setup.mock_parent_query_context_handler,
                test_setup.mock_object_proxies[0],
            ),
            call(
                test_setup.mock_parent_query_context_handler,
                test_setup.mock_object_proxies[1],
            ),
        ]
    )
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[0].remove
    ).assert_not_called()
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[1].remove
    ).assert_not_called()
    assert test_setup.mock_parent_query_context_handler.mock_calls == []


def test_multiple_object_proxies_add_remove():
    """
    This test adds multiple object proxy mocks to the ObjectProxyReferenceCountingBag and removes them afterwards.
    It expects besides the behavior of the adds, calls to remove on the two ObjectProxyReferenceCounter
    """
    test_setup = create_test_setup(proxy_count=2)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    bag.add(test_setup.mock_object_proxies[1])
    test_setup.reset_mock()
    bag.remove(test_setup.mock_object_proxies[0])
    bag.remove(test_setup.mock_object_proxies[1])
    assert test_setup.mock_parent_query_context_handler.mock_calls == []
    test_setup.mock_object_proxy_reference_counter_factory.assert_not_called()
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[0].remove
    ).assert_called_once()
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[1].remove
    ).assert_called_once()


def test_transfer_back_to_parent_query_handler_context_for_not_added_element():
    """
    This test calls transfer_back_to_parent_query_handler_context for a object that is not in the bag
    and expects that it fails.
    """
    test_setup = create_test_setup(proxy_count=1)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    with pytest.raises(KeyError):
        bag.transfer_back_to_parent_query_handler_context(
            test_setup.mock_object_proxies[0]
        )


def test_transfer_back_to_parent_query_handler_context_for_added_element():
    """
    This test calls transfer_back_to_parent_query_handler_context for an added object proxy.
    It expects that transfer_back_to_parent_query_handler_context on the corresponding reference counter is called.
    """
    test_setup = create_test_setup(proxy_count=1)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    test_setup.reset_mock()
    bag.transfer_back_to_parent_query_handler_context(test_setup.mock_object_proxies[0])
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[
            0
        ].transfer_back_to_parent_query_handler_context
    ).assert_called_once()
    assert (
        test_setup.mock_parent_query_context_handler.mock_calls == []
        and test_setup.mock_object_proxy_reference_counter_factory.mock_calls == []
    )


def test_transfer_back_to_parent_query_handler_context_after_remove():
    """
    This test calls transfer_back_to_parent_query_handler_context after the object proxy was removed from the bag.
    It expects that the call fails.
    """
    test_setup = create_test_setup(proxy_count=1)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    bag.remove(test_setup.mock_object_proxies[0])
    test_setup.reset_mock()
    with pytest.raises(KeyError):
        bag.transfer_back_to_parent_query_handler_context(
            test_setup.mock_object_proxies[0]
        )


def test_transfer_back_to_parent_query_handler_context_after_multiple_adds():
    """
    This test calls transfer_back_to_parent_query_handler_context after multiple adds for the object proxy.
    It expects the same behavior as after the first add.
    """
    test_setup = create_test_setup(proxy_count=1)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    bag.add(test_setup.mock_object_proxies[0])
    test_setup.reset_mock()
    bag.transfer_back_to_parent_query_handler_context(test_setup.mock_object_proxies[0])
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[
            0
        ].transfer_back_to_parent_query_handler_context
    ).assert_called_once()
    assert (
        test_setup.mock_parent_query_context_handler.mock_calls == []
        and test_setup.mock_object_proxy_reference_counter_factory.mock_calls == []
    )


def test_remove_after_transfer_back_to_parent_query_handler_context():
    """
    This test calls remove after it called transfer_back_to_parent_query_handler_context.
    It expects the remove to fail.
    """
    test_setup = create_test_setup(proxy_count=1)
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    bag.add(test_setup.mock_object_proxies[0])
    test_setup.reset_mock()
    bag.transfer_back_to_parent_query_handler_context(test_setup.mock_object_proxies[0])
    with pytest.raises(KeyError):
        bag.remove(test_setup.mock_object_proxies[0])


def test_add_after_transfer_back_to_parent_query_handler_context():
    """
    This test calls add after it called transfer_back_to_parent_query_handler_context.
    It expects the same behavior as after a normal add, because we transferred
    the object proxy back to the parent query_handler_context which is also the requirement
    to add it again.
    """
    test_setup = create_test_setup(proxy_count=1)
    # For this test we need allow the creation of a second reference counter for the same proxy count
    test_setup.mock_object_proxy_reference_counter_factory.side_effect = (
        test_setup.mock_object_proxy_reference_counters * 2
    )
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    bag.transfer_back_to_parent_query_handler_context(test_setup.mock_object_proxies[0])
    test_setup.reset_mock()
    bag.add(test_setup.mock_object_proxies[0])
    test_setup.mock_object_proxy_reference_counter_factory.assert_called_once_with(
        test_setup.mock_parent_query_context_handler, test_setup.mock_object_proxies[0]
    )
    assert test_setup.mock_parent_query_context_handler.mock_calls == []
    mock_cast(
        test_setup.mock_object_proxy_reference_counters[0].remove
    ).assert_not_called()


def test_contains_after_transfer_back_to_parent_query_handler_context():
    """
    This test calls __contains__ after it called transfer_back_to_parent_query_handler_context.
    It expects that the object proxy is not anymore in the bag.
    """
    test_setup = create_test_setup(proxy_count=1)
    # For this test we need allow the creation of a second reference counter for the same proxy count
    test_setup.mock_object_proxy_reference_counter_factory.side_effect = (
        test_setup.mock_object_proxy_reference_counters * 2
    )
    bag = ObjectProxyReferenceCountingBag(
        test_setup.mock_parent_query_context_handler,
        test_setup.mock_object_proxy_reference_counter_factory,
    )
    bag.add(test_setup.mock_object_proxies[0])
    bag.transfer_back_to_parent_query_handler_context(test_setup.mock_object_proxies[0])
    test_setup.reset_mock()
    assert test_setup.mock_object_proxies[0] not in bag

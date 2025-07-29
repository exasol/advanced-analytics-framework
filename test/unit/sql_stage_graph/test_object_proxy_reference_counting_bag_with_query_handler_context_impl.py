from pathlib import PurePosixPath

import pytest

from exasol.analytics.query_handler.context.top_level_query_handler_context import (
    TopLevelQueryHandlerContext,
)
from exasol.analytics.query_handler.graph.stage.sql.execution.object_proxy_reference_counting_bag import (
    ObjectProxyReferenceCountingBag,
)


@pytest.fixture
def context(top_level_query_handler_context_mock):
    return top_level_query_handler_context_mock


def test_single_add(context):
    """
    This test adds a object_proxy to a ObjectProxyReferenceCountingBag and checks if no objects got released
    """
    table_name = context.get_temporary_table_name()
    bag = ObjectProxyReferenceCountingBag(context)
    bag.add(table_name)
    assert len(context.cleanup_released_object_proxies()) == 0


def test_single_add_remove(context):
    """
    This test adds and removes a object_proxy to a ObjectProxyReferenceCountingBag
    and checks if one object proxy was released
    """
    table_name = context.get_temporary_table_name()
    bag = ObjectProxyReferenceCountingBag(context)
    bag.add(table_name)
    bag.remove(table_name)
    assert len(context.cleanup_released_object_proxies()) == 1


def test_single_add_remove_only_the_added_object_proxy_get_removed(context):
    """
    This test adds and removes a object_proxy to a ObjectProxyReferenceCountingBag. Further, it creates an
    additional object proxy which it doesn't add and check if only one proxy was released.
    """
    context.get_temporary_table_name()
    table_name = context.get_temporary_table_name()
    bag = ObjectProxyReferenceCountingBag(context)
    bag.add(table_name)
    bag.remove(table_name)
    assert len(context.cleanup_released_object_proxies()) == 1


def test_single_add_different_object_proxies_remove_one(context):
    """
    This test adds two object proxies to a ObjectProxyReferenceCountingBag and removes one.
    It then checks if one object proxy was released.
    """
    table_name1 = context.get_temporary_table_name()
    table_name2 = context.get_temporary_table_name()
    bag = ObjectProxyReferenceCountingBag(context)
    bag.add(table_name1)
    bag.add(table_name2)
    bag.remove(table_name1)
    assert len(context.cleanup_released_object_proxies()) == 1


def test_single_add_remove_add(context):
    """
    This test adds and removes a object proxy to a ObjectProxyReferenceCountingBag and then attempts to add it again.
    We expect the second add to fail, because top_level_query_handler_context already disowned the object.
    """
    table_name = context.get_temporary_table_name()
    bag = ObjectProxyReferenceCountingBag(context)
    bag.add(table_name)
    bag.remove(table_name)
    with pytest.raises(
        Exception, match="Object not owned by this ScopeQueryHandlerContext."
    ):
        bag.add(table_name)


def test_transfer_back_to_parent_query_handler_context_after_add(context):
    table_name = context.get_temporary_table_name()
    bag = ObjectProxyReferenceCountingBag(context)
    bag.add(table_name)
    bag.transfer_back_to_parent_query_handler_context(table_name)
    assert table_name not in bag
    context.release()
    assert len(context.cleanup_released_object_proxies()) == 1


def test_add_after_transfer_back_to_parent_query_handler_context(context):
    table_name = context.get_temporary_table_name()
    bag = ObjectProxyReferenceCountingBag(context)
    bag.add(table_name)
    bag.transfer_back_to_parent_query_handler_context(table_name)
    bag.add(table_name)
    assert table_name in bag

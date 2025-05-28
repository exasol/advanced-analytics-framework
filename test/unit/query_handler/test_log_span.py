import re
import uuid

from exasol.analytics.query_handler.query.select import LogSpan

SAMPLE_UUID = uuid.uuid4()


def is_uuid(id: uuid.UUID) -> bool:
    pattern = "-".join(f"[0-9a-f]{{{n}}}" for n in [8, 4, 4, 4, 12])
    return isinstance(id, uuid.UUID) and re.match(pattern, str(id))


def test_logspan():
    logspan = LogSpan("L1")
    assert logspan.name == "L1"
    assert is_uuid(logspan.id)
    assert logspan.parent is None


def test_logspan_with_id_and_parent():
    parent = LogSpan("parent")
    logspan = LogSpan("L2", SAMPLE_UUID, parent)
    assert logspan.name == "L2"
    assert logspan.id == SAMPLE_UUID
    assert logspan.parent == parent


def test_child():
    parent = LogSpan("parent")
    child = parent.child("child")
    assert child.name == "child"
    assert is_uuid(child.id)
    assert child.parent == parent


def test_child_with_id():
    parent = LogSpan("parent")
    child = parent.child("child", SAMPLE_UUID)
    assert child.name == "child"
    assert child.id == SAMPLE_UUID
    assert child.parent == parent

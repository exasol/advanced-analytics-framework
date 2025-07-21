from typing import (
    Type,
    get_args,
)

import pytest
from polyfactory.factories.pydantic_factory import ModelFactory
from pydantic.fields import FieldInfo

from exasol.analytics.udf.communication.messages import *
from exasol.analytics.udf.communication.serialization import (
    deserialize_message,
    serialize_message,
)

base_message_subclasses = BaseMessage.__subclasses__()


@pytest.mark.parametrize("message_class", base_message_subclasses)
def test_message_serialization(message_class: Type):
    factory = ModelFactory.create_factory(model=message_class)
    message = factory.build()
    byte_string = serialize_message(message)
    obj = deserialize_message(byte_string, Message)
    assert message == obj.root


@pytest.mark.parametrize("message_class", base_message_subclasses)
def test_message_type(message_class: Type):
    factory = ModelFactory.create_factory(model=message_class)
    message = factory.build()
    assert (
        "message_type" in message.__dict__
        and message.message_type == message.__class__.__name__
    )


def test_all_base_message_subclasses_are_registered_in_root_field_of_message():
    root_field: FieldInfo = Message.model_fields["root"]
    classes_in_root_field = set(get_args(root_field.annotation))
    assert classes_in_root_field == set(base_message_subclasses)

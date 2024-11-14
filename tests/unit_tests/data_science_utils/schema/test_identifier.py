import pytest

from exasol.analytics.schema import (
    ExasolIdentifierImpl,
    ExasolIdentifier,
)


class TestSchemaElement(ExasolIdentifierImpl):
    __test__ = False

    def __init__(self, name: str):
        super().__init__(name)

    @property
    def fully_qualified(self) -> str:
        raise NotImplemented()

    def __eq__(self, other):
        raise NotImplemented()

    def __repr__(self):
        raise NotImplemented()


@pytest.mark.parametrize("test_name",
                         [
                             "A",
                             "a",
                             "B_",
                             "Z1",
                             "Q\uFE33",
                             "Ü",
                             "1"
                         ])
def test_name_valid(test_name):
    TestSchemaElement(test_name)


@pytest.mark.parametrize("test_name",
                         [
                             ".",
                             "A.s"
                             "_",
                             ",",
                             ";",
                             ":",
                             "\uFE33",
                             '"',
                             'A"',
                             "A'",
                             "A,",
                             "A;",
                             "A:"
                         ])
def test_name_invalid(test_name):
    with pytest.raises(ValueError):
        TestSchemaElement(test_name)


@pytest.mark.parametrize("name,expected_quoted_name",
                         [
                             ('ABC', '"ABC"'),
                             # ('A"BC', '"A""BC"'), names with double quotes at the moment not valid
                             ('abc', '"abc"')
                         ])
def test_quote(name, expected_quoted_name):
    quoted_name = TestSchemaElement(name).quoted_name
    assert quoted_name == expected_quoted_name


def test_get_name():
    element = TestSchemaElement("abc")
    assert element.name == "abc"


def test_set_new_name_fail():
    element = TestSchemaElement("abc")
    with pytest.raises(AttributeError) as c:
        element.name = "edf"

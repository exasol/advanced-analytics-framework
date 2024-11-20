import unicodedata

from typeguard import typechecked
from typing import Optional

from exasol.analytics.schema.exasol_identifier import ExasolIdentifier


class UnicodeCategories:
    UPPERCASE_LETTER = "Lu"
    LOWERCASE_LETTER = "Ll"
    TITLECASE_LETTER = "Lt"
    MODIFIER_LETTER = "Lm"
    OTHER_LETTER = "Lo"
    LETTER_NUMBER = "Nl"
    NON_SPACING_MARK = "Mn"
    COMBINING_SPACING_MARK = "Mc"
    DECIMAL_DIGIT_NUMBER = "Nd"
    CONNECTOR_PUNCTUATION = "Pc"
    FORMAT = "Cf"


class ExasolIdentifierImpl(ExasolIdentifier):

    @typechecked
    def __init__(self, name: Optional[str]):
        if not self._validate_name(name):
            raise ValueError(f"Name '{name}' is not valid")
        self._name = str(name)

    @property
    def name(self) -> str:
        return self._name

    @property
    def quoted_name(self) -> str:
        return f'"{self._name}"'

    @classmethod
    def _validate_name(cls, name: Optional[str]) -> bool:
        if name is None or name == "":
            return False
        if not cls._validate_first_character(name[0]):
            return False
        for c in name[1:]:
            if not cls._validate_follow_up_character(c):
                return False
        return True

    @classmethod
    def _validate_first_character(cls, character: str) -> bool:
        unicode_category = unicodedata.category(character)
        return (
            unicode_category == UnicodeCategories.UPPERCASE_LETTER
            or unicode_category == UnicodeCategories.LOWERCASE_LETTER
            or unicode_category == UnicodeCategories.TITLECASE_LETTER
            or unicode_category == UnicodeCategories.MODIFIER_LETTER
            or unicode_category == UnicodeCategories.OTHER_LETTER
            or unicode_category == UnicodeCategories.LETTER_NUMBER
            or unicode_category == UnicodeCategories.DECIMAL_DIGIT_NUMBER
        )

    @classmethod
    def _validate_follow_up_character(cls, character: str) -> bool:
        unicode_category = unicodedata.category(character)
        return (
            unicode_category == UnicodeCategories.UPPERCASE_LETTER
            or unicode_category == UnicodeCategories.LOWERCASE_LETTER
            or unicode_category == UnicodeCategories.TITLECASE_LETTER
            or unicode_category == UnicodeCategories.MODIFIER_LETTER
            or unicode_category == UnicodeCategories.OTHER_LETTER
            or unicode_category == UnicodeCategories.LETTER_NUMBER
            or unicode_category == UnicodeCategories.NON_SPACING_MARK
            or unicode_category == UnicodeCategories.COMBINING_SPACING_MARK
            or unicode_category == UnicodeCategories.DECIMAL_DIGIT_NUMBER
            or unicode_category == UnicodeCategories.CONNECTOR_PUNCTUATION
            or unicode_category == UnicodeCategories.FORMAT
            or character == "\u00B7"
        )

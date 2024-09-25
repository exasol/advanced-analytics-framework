from dataclasses import fields

import typeguard
from typeguard import TypeCheckError

def check_dataclass_types(datacls):
    for field in fields(datacls):
        try:
            typeguard.check_type(value=datacls.__dict__[field.name],
                                 expected_type=field.type)
        except TypeCheckError as e:
            raise TypeCheckError(f"Field '{field.name}' has wrong type: {e}")

from dataclasses import fields

import typeguard


def check_dataclass_types(datacls):
    for field in fields(datacls):
        typeguard.check_type(
            value=datacls.__dict__[field.name],
            expected_type=field.type,
        ) # removed argname=field.name

from typing import cast, Any
from unittest.mock import Mock


def mock_cast(obj: Any) -> Mock:
    return cast(Mock, obj)

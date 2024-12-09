import json
from datetime import datetime

from .config import RootConfig


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            try:
                return obj.decode("ascii")
            except UnicodeDecodeError:
                return obj.hex()
        return super().default(obj)


__all__ = ["RootConfig", "DateTimeEncoder"]

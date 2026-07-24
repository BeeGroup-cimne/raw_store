import logging
import os

from pythonjsonlogger.json import JsonFormatter

_CLIENT = os.environ.get("CLIENT")
PROJECT = f"raw-store-{_CLIENT}" if _CLIENT else "raw-store"


class _ContextFilter(logging.Filter):
    def filter(self, record):
        record.project = PROJECT
        return True


def setup_logging(level=logging.INFO):
    """Configure a single JSON handler on the root logger. Call once per entry point."""
    root = logging.getLogger()
    root.setLevel(level)
    handler = logging.StreamHandler()
    formatter = JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s %(project)s",
        rename_fields={"asctime": "timestamp", "levelname": "level"},
    )
    handler.setFormatter(formatter)
    handler.addFilter(_ContextFilter())
    root.addHandler(handler)

"""Compatibility layer for simcc.core.db.model.

All models have been modularized into domain packages
under simcc.core.db.models. This module re-exports all
models and registry to maintain full backward compatibility.
"""

from simcc.core.db.models import *  # noqa: F401, F403
from simcc.core.db.models import __all__  # noqa: F401
from simcc.core.db.models.base import table_registry  # noqa: F401

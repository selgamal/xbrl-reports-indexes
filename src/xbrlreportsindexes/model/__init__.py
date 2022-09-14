"""All model objects"""
from __future__ import annotations

from xbrlreportsindexes.model import base_model
from xbrlreportsindexes.model import esef_index_model
from xbrlreportsindexes.model import sec_feeds_model
from xbrlreportsindexes.model.base_model import Base

ESEF = esef_index_model
SEC = sec_feeds_model
BASE_M = base_model
BASE = Base

from typing import Callable

from sqlalchemy.ext.declarative import DeclarativeMeta

from .main import UHQL, UHQLBaseDataProvider
from .main.sqlalchemy import UHQLSqlAlchemyDataProvider

from .main.basetypes import UHQLException, UHQLUserRequest


# Default Implementation
def UHQL_SqlAlchemy(
    model_base: DeclarativeMeta, dbsession=None, extra_type_injector=None, can_func=None, post_update_hook=None
) -> UHQL:
    return UHQL(
        UHQLSqlAlchemyDataProvider(model_base, dbsession), extra_type_injector, can_func, post_update_hook
    )

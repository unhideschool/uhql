from typing import Callable

from sqlalchemy.ext.declarative import DeclarativeMeta

from .main import UHQL, UHQLBaseDataProvider

from .main.sqlalchemy import UHQLSqlAlchemyDataProvider


# Default Implementation
def UHQL_SqlAlchemy(model_base: DeclarativeMeta, dbsession=None, extra_type_injector=None) -> UHQL:
    return UHQL(UHQLSqlAlchemyDataProvider(model_base, dbsession), extra_type_injector)

from abc import abstractmethod

from sqlalchemy.orm import Query

from .basestructures import UHFilterTypes
from .basetypes import UHQLBaseDataProvider, UHQLUserRequest, UHQLBaseFilter, UHQLException

from typing import Type

T = Type

from sqlalchemy.ext.declarative import DeclarativeMeta


class UHQLSqlAlchemyDataProvider(UHQLBaseDataProvider):
    def __init__(self, model_base: DeclarativeMeta, dbsession=None):
        self.model_base = model_base
        self.dbsession = dbsession

    def get_list(self, req: UHQLUserRequest):

        base_query = self.__get(req)
        results = base_query.all()

        return results

    def get_one(self, req: UHQLUserRequest):

        base_query = self.__get(req)
        results = base_query.one_or_none()

        return results

    def __get(self, req: UHQLUserRequest) -> Query:

        resource = req.resource
        schema = req.schema
        page = req.page
        perpage = req.perpage
        filters = req.filters
        order_by = req.order_by

        dbclass = self.__get_class(resource)
        base_query = self.dbsession.query(dbclass)

        valid_filters = [x.value for x in UHFilterTypes]
        for _filter in filters:
            _field = _filter.field
            _value = _filter.value
            _op = _filter.op

            if _filter.op in valid_filters:
                base_query = base_query.filter(eval(f"getattr(dbclass, _field) {_op} _value"))
                continue

            # Invalid operation specified.
            raise UHQLException("Invalid Filter")

        if page:
            page -= 1
            base_query = base_query.order_by(order_by).limit(perpage).offset(page * perpage)

        return base_query

    def __get_class(self, resource: str) -> T:

        myclass = None

        for c in self.model_base._decl_class_registry.values():
            if hasattr(c, "__table__") and c.__table__.name == resource:
                myclass = c

        if not myclass:
            raise Exception(f'Resource not found: "{resource}".')

        return myclass

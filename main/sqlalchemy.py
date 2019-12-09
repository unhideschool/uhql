from abc import abstractmethod

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

        # Split parameters from REQ
        resource = req.resource
        schema = req.schema
        page = req.page
        perpage = req.perpage
        filters = req.filters
        order_by = req.order_by

        # response
        response = []

        dbclass = self.__get_class(resource)
        q = self.dbsession.query(dbclass)

        valid_filters = [x.value for x in UHFilterTypes]
        for _filter in filters:
            _field = _filter.field
            _value = _filter.value
            _op = _filter.op

            if _filter.op in valid_filters:
                # TODO: invalid field attribute handling (AttributeError)
                q = q.filter(eval(f"getattr(dbclass, _field) {_op} _value"))
                continue

            # Invalid operation specified.
            raise UHQLException("Invalid Filter")

        if page:
            # todo: fix paginacao contando a partir de "1"
            q = q.order_by(order_by).limit(perpage).offset(page * perpage)

        results = q.all()

        return results

    def __get_class(self, resource: str) -> T:

        myclass = None

        for c in self.model_base._decl_class_registry.values():
            if hasattr(c, "__table__") and c.__table__.name == resource:
                myclass = c

        if not myclass:
            raise Exception(f'Resource not found: "{resource}".')

        return myclass

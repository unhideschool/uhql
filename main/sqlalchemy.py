import re
from abc import abstractmethod

from sqlalchemy.orm import Query

from .basestructures import UHFilterTypes
from .basetypes import (
    UHQLBaseDataProvider,
    UHQLUserRequest,
    UHQLBaseFilter,
    UHQLException,
)

from typing import Type, Dict, Callable, List

T = Type

from sqlalchemy.ext.declarative import DeclarativeMeta


def catch_pattern(regexp: str = ""):
    def catch_pattern_decorator(f):
        # set catch pattern
        f.catch_pattern = regexp
        return f

    return catch_pattern_decorator


class UHQLSqlAlchemyDataProvider(UHQLBaseDataProvider):
    def __init__(self, model_base: DeclarativeMeta, dbsession=None):
        self.model_base = model_base
        self.dbsession = dbsession

        # generate get handlers
        self.get_list_handlers = self.__gethandlers("get_list")

        pass

    def __gethandlers(self, pat="") -> Dict[str, Callable]:

        full_pattern = f"_{self.__class__.__name__}__{pat.strip()}"

        candidates = [
            getattr(self, b)
            for b in dir(self)
            if re.match(full_pattern, b)
            and getattr(getattr(self, b), "catch_pattern", None) is not None
        ]

        print(candidates)

        return candidates

    @staticmethod
    def get_best_handler(candlist: List[Callable], resourcename: str):
        p = sorted(
            [
                (re.match(x.catch_pattern, resourcename).span()[1], x)
                for x in candlist
                if re.match(x.catch_pattern, resourcename)
            ],
            key=lambda x: x[0],
            reverse=True,
        )
        return p[0][1] if p else None

    def get_list(self, req: UHQLUserRequest) -> List[Dict]:

        get_handlers = self.__gethandlers("get_list")
        handler = self.get_best_handler(get_handlers, req.resource)

        return handler(req)

    def get_one(self, req: UHQLUserRequest):

        base_query = self.__get_generic_sqlalchemy(req)
        results = base_query.one_or_none()

        return results

    @catch_pattern("!tables")
    def __get_list_tables(self, req: UHQLUserRequest):
        # TODO: virtual method
        pass

    def __get_generic_sqlalchemy(self, req: UHQLUserRequest) -> Query:

        resource = req.resource
        schema = req.schema
        page = req.page
        perpage = req.perpage
        filters = req.filters
        order_by_criterion = req.order_by

        db_class = self.__get_sqlalchemy_class_from_tablename(resource)
        base_query = self.dbsession.query(db_class)

        valid_filters = [x.value for x in UHFilterTypes]
        for _filter in filters:
            _field = _filter.field
            _value = _filter.value
            _op = _filter.op

            if _filter.op in valid_filters:
                base_query = base_query.filter(
                    eval(f"getattr(dbclass, _field) {_op} _value")
                )
                continue

            # Invalid operation specified.
            raise UHQLException("Invalid Filter")

        if order_by_criterion:
            base_query = base_query.order_by(order_by_criterion)

        if page > 0:
            page -= 1
            base_query = base_query.limit(perpage).offset(page * perpage)

        return base_query

    @catch_pattern("")
    def __get_list_sqlalchemy(self, req: UHQLUserRequest):

        results = self.__get_generic_sqlalchemy(req).all()
        return results

    def __get_one_sqlalchemy(self, req: UHQLUserRequest):

        results = self.__get_generic_sqlalchemy(req).one_or_none()
        return results

    def __get_sqlalchemy_class_from_tablename(self, resource: str) -> T:

        tableq = [
            x
            for x in self.model_base._decl_class_registry.values()
            if hasattr(x, "__table__") and x.__table__.name == resource
        ]

        if tableq:
            return tableq[0]

        raise Exception(f'Resource not found: "{resource}".')

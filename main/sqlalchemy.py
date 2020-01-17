import re
from typing import Type, Dict, Callable, List

import sqlalchemy
from sqlalchemy import Table
from sqlalchemy.orm import Query
from sqlalchemy.util import KeyedTuple

from .basestructures import UHFilterTypes
from .basetypes import UHQLBaseDataProvider, UHQLUserRequest, UHQLException, UHQLBaseResultSet, UHQLTableListResultSet

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

    def get_dict_from_obj(self, obj) -> dict:
        """
        @param obj:
        @return: dict from obj
        """

        d = None
        if isinstance(obj, self.model_base):
            d = {c.name: getattr(obj, c.name) for c in obj.__table__.columns}
        elif isinstance(obj, dict):
            d = obj

        print("aeeee")

        if d:
            return d

        raise Exception(f"Cannot generate dict -- implement type \"{type(obj)}\"...")

    def get_list(self, req: UHQLUserRequest) -> List[Dict]:

        get_handlers = self.__gethandlers("get_list")
        handler = self.get_best_handler(get_handlers, req.resource)

        resultset: UHQLBaseResultSet = handler(req)

        data = resultset.to_listdict()

        return data

    def get_one(self, req: UHQLUserRequest):

        base_query = self.__get_generic_sqlalchemy(req)
        results = base_query.one_or_none()

        return results

    def create(self, req: UHQLUserRequest):

        db_class = self.__get_sqlalchemy_queryableobj_from_tablename(req.resource)

        for key in req.schema:
            if hasattr(db_class, key):
                continue
            else:
                raise UHQLException(f"Invalid field={key}")

        obj = db_class(**req.schema)
        self.dbsession.add(obj)
        self.dbsession.commit()

        return obj

    def __create(self, db_class, jsonrequest):

        obj = db_class(**jsonrequest)
        self.dbsession.add(obj)
        self.dbsession.commit()

        return obj

    @catch_pattern("!tables")
    def __get_list_tables(self, req: UHQLUserRequest) -> UHQLBaseResultSet:

        column_descriptions = [
            {
                "name": "id",
                "type": "integer"
            },
            {
                "name": "name",
                "type": "string"
            }
        ]

        

        def build_tabledict_from_satable(id: int, t: Table):
            r = {
                "id": id,
                "name": t.name,
                "fields": [
                    (x.name, x.primary_key, str(x.type))
                    for x in t.columns
                ],
            }

            return r

        data = [
            build_tabledict_from_satable(tableid, tableinstance)
            for tableid, tableinstance in list(
            enumerate(self.model_base.metadata.tables.values())
        )
        ]

        r = UHQLTableListResultSet(data, column_descriptions)

        return r

    def __get_generic_sqlalchemy(self, req: UHQLUserRequest) -> Query:

        resource = req.resource
        schema = req.schema
        page = req.page
        perpage = req.perpage
        filters = req.filters
        order_by_criterion = req.order_by

        db_object_or_class = self.__get_sqlalchemy_queryableobj_from_tablename(resource)
        base_query = self.dbsession.query(db_object_or_class)

        valid_filters = [x.value for x in UHFilterTypes]
        for _filter in filters:
            _field = _filter.field
            _value = _filter.value
            _op = _filter.op

            if _filter.op in valid_filters:
                base_query = base_query.filter(
                    eval(f"getattr(db_object_or_class, _field) {_op} _value")
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
    def __get_list_sqlalchemy(self, req: UHQLUserRequest) -> UHQLBaseResultSet:

        basequery = self.__get_generic_sqlalchemy(req)

        results = UHQLBaseResultSet(basequery.all(), basequery.column_descriptions)

        return results

    def __get_one_sqlalchemy(self, req: UHQLUserRequest):

        results = self.__get_generic_sqlalchemy(req).one_or_none()
        return results

    def __get_sqlalchemy_queryableobj_from_tablename(self, resource: str) -> T:

        print("10")

        # do we have a class?
        for c in self.model_base._decl_class_registry.values():
            if hasattr(c, '__tablename__') and c.__tablename__ == resource:
                # return class
                return c

        # no class? maybe we have a pure table.
        if resource in self.model_base.metadata.tables:
            # return table
            t = self.model_base.metadata.tables[resource]
            return t

        # Error
        raise Exception(f'Resource not found: "{resource}".')

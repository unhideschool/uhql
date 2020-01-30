import re
from typing import Type, Dict, Callable, List

from sqlalchemy import Table
from sqlalchemy.orm import Query

from .basestructures import UHFilterTypes
from .basetypes import UHQLBaseDataProvider, UHQLUserRequest, UHQLException

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

        if isinstance(obj, self.model_base):
            d = {c.name: getattr(obj, c.name) for c in obj.__table__.columns}
        elif isinstance(obj, dict):
            d = obj

        return d

    def get_list(self, req: UHQLUserRequest) -> List[Dict]:

        get_handlers = self.__gethandlers("get_list")
        handler = self.get_best_handler(get_handlers, req.resource)

        return handler(req)

    def get_one(self, req: UHQLUserRequest):

        base_query = self.__get_generic_sqlalchemy(req)
        results = base_query.one_or_none()

        return results

    def create(self, req: UHQLUserRequest):

        db_class = self.__get_sqlalchemy_class_from_tablename(req.resource)

        for key in req.schema:
            if hasattr(db_class, key):
                continue
            else:
                raise UHQLException(f"Invalid field={key}")

        obj = db_class(**req.schema)
        self.dbsession.add(obj)
        self.dbsession.commit()

        return obj

    def update(self, req: UHQLUserRequest):
        db_class = self.__get_sqlalchemy_class_from_tablename(req.resource)

        base_query = self.__get_generic_sqlalchemy(req)
        obj = base_query.one_or_none()

        if not obj:
            raise UHQLException(f"Object not found")

        for key in req.schema:
            try:
                if hasattr(db_class, key):
                    new_value = req.schema[key]

                    if getattr(obj, key) != new_value:
                        setattr(obj, key, new_value)
                else:
                    raise UHQLException(f"Invalid field={key}")

            except Exception as ex:
                raise UHQLException(f"Invalid field={key}")

        self.dbsession.commit()

        return obj

    def delete(self, req: UHQLUserRequest):

        base_query = self.__get_generic_sqlalchemy(req)
        obj = base_query.one_or_none()

        if not obj:
            raise UHQLException(f"Object not found")

        self.dbsession.delete(obj)
        self.dbsession.commit()

        return obj

    @catch_pattern("!tables")
    def __get_list_tables(self, req: UHQLUserRequest):

        def build_tabledict_from_satable(id: int, t: Table):
            r = {
                id: id,
                "name": t.name,
                "fields": [
                    (x.name, x.primary_key, str(x.type))
                    for x in t.columns
                ],
            }

            return r

        return [
            build_tabledict_from_satable(tableid, tableinstance)
            for tableid, tableinstance in list(
                enumerate(self.model_base.metadata.tables.values())
            )
        ]

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

                if _filter.op == UHFilterTypes.LIKE.value:
                    base_query = base_query.filter(

                        eval(f"getattr(db_class, _field).ilike(_value)")
                    )

                else:

                    base_query = base_query.filter(
                        eval(f"getattr(db_class, _field) {_op} _value")
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

        def get_class_by_tablename(Base, tablename):
            """Return class reference mapped to table.

            :param tablename: String with name of table.
            :return: Class reference or None.
            """
            for c in Base._decl_class_registry.values():
                if hasattr(c, '__tablename__') and c.__tablename__ == tablename:
                    return c

        a = get_class_by_tablename(self.model_base, resource)

        return a

        # tableq = [
        #     x
        #     for x in self.model_base._decl_class_registry.values()
        #     if hasattr(x, "__table__") and x.__table__.name == resource
        # ]
        #
        # if tableq:
        #     return tableq[0]

        raise Exception(f'Resource not found: "{resource}".')

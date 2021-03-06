from typing import Type, Dict, List

T = Type


from abc import ABCMeta, abstractmethod
from enum import Enum

from .types import UHOperationTypes
from functools import partial


class UHQL:
    def __init__(self, base_class: T, dbsession=None, extra_type_injector=None):
        self.base_class = base_class
        self.dbsession = dbsession
        self.extra_type_injector = extra_type_injector

    @abstractmethod
    def get_list(self, resource: str, schema: dict, page, perpage, order_by, filters) -> List[Dict]:

        response = []

        cls = self.__get_class(resource)
        q = self.dbsession.query(cls)

        if filters:
            for key, value in filters.items():
                q = q.filter(getattr(cls, key) == value)

        if page:
            q = q.order_by(order_by).limit(perpage).offset(page * perpage)

        results = q.all()
        response = [self.__build_with_schema(result, schema) for result in results]

        return response

    def __build_with_schema(self, obj, schema):
        d = dict()

        if 'properties' not in schema:
            return obj

        for key in schema['properties'].keys():

            if not hasattr(obj, key):
                # key not in object
                continue

            prop = getattr(obj, key)

            # Try to use our extra type injector
            extra_types_response = self.extra_type_injector(prop, key)
            if extra_types_response:
                d[key] = extra_types_response
                continue

            if 'type' not in schema['properties'][key]:
                d[key] = prop

            elif schema['properties'][key]['type'] == 'object':

                d[key] = self.__build_with_schema(prop, schema['properties'][key])

            elif schema['properties'][key]['type'] == 'array':

                d[key] = [self.__build_with_schema(item, schema['properties'][key]['items'][0]) for item in prop]

            else:
                d[key] = prop

        return d

    def __get_class(self, resource: str) -> T:

        myclass = None

        for c in self.base_class._decl_class_registry.values():
            if hasattr(c, '__table__') and c.__table__.name == resource:
                myclass = c

        if not myclass:
            pass  # TODO: ERROR.

        return myclass


from typing import Callable, TypeVar

import jsonschema
from jsonschema import ValidationError

from .basetypes import UHQLBaseDataProvider, UHQLUserRequest, UHQLException
from .data import JSONContracts
from .tools import logged_method

T = TypeVar("T")


class UHQL:
    def __init__(
            self,
            dataprovider: UHQLBaseDataProvider,
            extra_type_injector: Callable[[dict, str], T] = None,
            can_func: Callable[[UHQLUserRequest], bool] = None,
            post_func: Callable[[UHQLUserRequest], bool] = None,
    ):
        self.d: UHQLBaseDataProvider = dataprovider
        self.extra_type_injector = extra_type_injector
        self.can_func = can_func
        self.post_func = post_func

    # def build_from_schema(self, obj, schema):
    #     d = dict()
    #
    #     if "properties" not in schema:
    #         return obj
    #
    #     for key in schema["properties"].keys():
    #
    #         if not hasattr(obj, key):
    #             # key not in object
    #             continue
    #
    #         prop = getattr(obj, key)
    #
    #         # Try to use our extra type injector
    #         if callable(self.extra_type_injector):
    #             extra_type_injector_response = self.extra_type_injector(prop, key)
    #             if extra_type_injector_response:
    #                 d[key] = extra_type_injector_response
    #                 continue
    #
    #         if "type" not in schema["properties"][key]:
    #             d[key] = prop
    #
    #         elif schema["properties"][key]["type"] == "object":
    #
    #             d[key] = self.build_from_schema(prop, schema["properties"][key])
    #
    #         elif schema["properties"][key]["type"] == "array":
    #
    #             d[key] = [
    #                 self.build_from_schema(item, schema["properties"][key]["items"][0])
    #                 for item in prop
    #             ]
    #
    #         else:
    #             d[key] = prop
    #
    #     return d

    def build_from_schema(self, obj, schema):
        d = dict()

        if schema == {}:
            return self.d.get_dict_from_obj(obj)

        for key in schema:

            if hasattr(obj, key):
                prop = getattr(obj, key)
            else:
                continue

            # Try to use our extra type injector
            if callable(self.extra_type_injector):
                extra_type_injector_response = self.extra_type_injector(prop, key)
                if extra_type_injector_response:
                    d[key] = extra_type_injector_response
                    continue

            if not schema[key]:
                d[key] = prop

            elif type(schema[key]) == dict:
                d[key] = self.build_from_schema(prop, schema[key])

            elif isinstance(prop, list):
                d[key] = [
                    self.build_from_schema(item, schema[key][0])
                    for item in prop
                ]

            else:
                d[key] = prop

        return d

    @logged_method
    def get_list(self, *, jsonrequest: dict):
        """
        Get a resource list.

        @param jsonrequest: dict
        @return: resource: List
        """

        try:
            jsonschema.validate(jsonrequest, JSONContracts.uhql_request_contract)
        except ValidationError as e:
            raise UHQLException(e.message)

        user_request = UHQLUserRequest(jsonrequest)

        if callable(self.can_func):
            if not self.can_func(user_request):
                raise UHQLException("Unauthorized")

        get_list_data = [
            self.build_from_schema(obj, user_request.schema)
            for obj in self.d.get_list(user_request)
        ]

        if callable(self.post_func):
            self.post_func(user_request)

        return get_list_data

    @logged_method
    def get_one(self, *, jsonrequest: dict):
        """
        get just one object by a resource.

        @param jsonrequest: dict
        @return: resource: dict
        """

        try:
            jsonschema.validate(jsonrequest, JSONContracts.uhql_request_contract)
        except ValidationError as e:
            raise UHQLException(e.message)

        user_request = UHQLUserRequest(jsonrequest)

        if callable(self.can_func):
            if not self.can_func(user_request):
                raise UHQLException("Unauthorized")

        obj = self.d.get_one(user_request)
        get_one_data = self.build_from_schema(obj, user_request.schema)

        if callable(self.post_func):
            self.post_func(user_request)

        return get_one_data

    @logged_method
    def create(self, *, jsonrequest: dict):
        """
        create a new object by a schema.

        @param jsonrequest: dict
        @return: dict
        """

        try:
            jsonschema.validate(jsonrequest, JSONContracts.uhql_request_contract)
        except ValidationError as e:
            raise UHQLException(e.message)

        user_request = UHQLUserRequest(jsonrequest)

        if callable(self.can_func):
            if not self.can_func(user_request):
                raise UHQLException("Unauthorized")

        obj = self.d.create(user_request)
        create_data = self.build_from_schema(obj, user_request.schema)

        if callable(self.post_func):
            self.post_func(user_request)

        return create_data

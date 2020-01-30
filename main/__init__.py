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
            post_update_hook: Callable[[UHQLUserRequest, T], bool] = None,
    ):
        self.d: UHQLBaseDataProvider = dataprovider
        self.extra_type_injector = extra_type_injector
        self.can_func = can_func
        self.post_update_hook = post_update_hook

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

        if callable(self.post_update_hook):
            self.post_update_hook(user_request, get_list_data)

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

        if callable(self.post_update_hook):
            self.post_update_hook(user_request, obj)

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

        if callable(self.post_update_hook):
            self.post_update_hook(user_request, obj)

        return create_data

    @logged_method
    def update(self, *, jsonrequest: dict):
        """
        update an object by a schema.

        @param jsonrequest: dict
        @return: dict
        """
        user_request = UHQLUserRequest(jsonrequest)

        if callable(self.can_func):
            if not self.can_func(user_request):
                raise UHQLException("Unauthorized")

        result = self.d.update(user_request)

        if callable(self.post_update_hook):
            self.post_update_hook(user_request, result)

        return result

    @logged_method
    def delete(self, *, jsonrequest: dict):
        """
        delete an object.

        @param jsonrequest: dict
        @return: dict
        """
        user_request = UHQLUserRequest(jsonrequest)

        if callable(self.can_func):
            if not self.can_func(user_request):
                raise UHQLException("Unauthorized")

        result = self.d.delete(user_request)

        if callable(self.post_update_hook):
            self.post_update_hook(user_request, result)

        return result
# TODO: Criar plugin de Auth do UNHIDEAPI (mock: return true)
# TODO: Criar plugin de DataProvider do UNHIDEAPI
from abc import ABCMeta, abstractmethod
from decimal import Decimal
from typing import Type, List, Union

T = Type


class UHQLException(Exception):
    pass


class UHQLBaseFilter:
    def __init__(
            self, field: str, op: str, value: Union[str, Decimal, int, bool, float]
    ):
        self.field = field
        self.op = op
        self.value = value


class UHQLUserRequest:
    def __init__(self, r: dict, page: int = 1, perpage: int = 250, order_by: str = ''):

        # Parse Request Object
        self.resource: str = r["resource"]
        self.schema: str = r["schema"] if 'schema' in r else {}

        self.page: int = r["page"] if 'page' in r else page
        self.perpage: int = r["perpage"] if 'perpage' in r else perpage
        self.order_by: str = r["order_by"] if 'order_by' in r else order_by

        self.filters: List[UHQLBaseFilter] = []

        try:
            filterlist = r["filters"]
        except KeyError:
            filterlist = []

        try:
            for f in filterlist:
                field = f["field"]
                op = f["op"]
                value = f["value"]
                self.filters.append(UHQLBaseFilter(field, op, value))
        except KeyError as e:
            raise UHQLException("Invalid Filter Format")


class UHQLBaseDataProvider(metaclass=ABCMeta):
    def login(self):
        """Implement this to connect"""
        pass

    def logoff(self):
        """Implement this to disconnect"""
        pass

    def can(self, req: UHQLUserRequest) -> bool:
        """Can the user realize this operation on this resource??"""
        return True

    @abstractmethod
    def get_list(self, req: UHQLUserRequest):
        pass

# TODO: Criar plugin de Auth do UNHIDEAPI (mock: return true)
# TODO: Criar plugin de DataProvider do UNHIDEAPI
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from typing import Type, List, Union, Dict, Any

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
    def __init__(self, r: dict, page: int = 1, perpage: int = 250, order_by: str = ""):

        # Parse Request Object
        self.resource: str = r["resource"]
        self.schema: str = r["schema"] if "schema" in r else {}

        self.page: int = r["page"] if "page" in r else page
        self.perpage: int = r["perpage"] if "perpage" in r else perpage
        self.order_by: str = r["order_by"] if "order_by" in r else order_by

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
        return bool(req)

    def get_dict_from_obj(self, obj) -> dict:
        if not isinstance(obj, dict):
            raise UHQLException(
                "Your object need to be converted to dict -- please implement this function"
            )
        return obj

    @abstractmethod
    def get_list(self, req: UHQLUserRequest) -> List[Dict]:
        pass

    @abstractmethod
    def get_one(self, req: UHQLUserRequest):
        pass

    @abstractmethod
    def create(self, req: UHQLUserRequest):
        pass


@dataclass
class UHQLColumnType:
    name: str
    type: str


class UHQLBaseResultSet:
    def __init__(self, data: List[Any], column_descriptions: List[Dict]):

        self.data = data

        self.columns = []
        for column_description in column_descriptions:
            self.columns.append(
                UHQLColumnType(column_description["name"], column_description["type"])
            )

    def to_listdict(self) -> List[Dict]:
        r = []

        for result in self.data:

            d = {}
            for column in self.columns:
                d[column.name] = getattr(result, column.name)

            r.append(d)

        return r

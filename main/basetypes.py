# TODO: Criar plugin de Auth do UNHIDEAPI (mock: return true)
# TODO: Criar plugin de DataProvider do UNHIDEAPI
from abc import ABCMeta, abstractmethod
from typing import Type
T = Type

from .types import UHOperationTypes


class UHBaseAuthProvider(meta=ABCMeta):
    @abstractmethod
    def can(self, user: T, resource: str, operation: UHOperationTypes) -> bool:
        """Implement this method """
        return True




class UHQL(meta=ABCMeta):
    def __init__(self,):
        pass

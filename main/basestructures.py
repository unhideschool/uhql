from enum import Enum


class UHOperationTypes(Enum):
    CREATE = 1
    READ = 2
    UPDATE = 3
    DELETE = 4


class UHFilterTypes(Enum):
    EQUAL = "=="
    DIFFERENT = "!="
    GREATER_THAN = ">"
    LESSER_THAN = "<"
    LIKE = 'like'

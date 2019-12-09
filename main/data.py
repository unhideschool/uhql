from dataclasses import dataclass
from typing import List


class JSONContracts:
    uhql_contract = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Get list",
        "type": "object",
        "properties": {
            "resource": {
                "type": "string"
            },
            "page": {
                "type": "integer"
            },
            "perpage": {
                "type": "integer"
            },
            "filters": {
                "type": "array",
                "items": [
                    {
                        "type": "object",
                        "properties": {
                            "field": {
                                "description": "field name to apply this filter",
                                "type": "string"
                            },
                            "operation": {
                                "type": "string",
                                "enum": ["=", "!=", ">", "<"]  # TODO: definir corretamente quais os tipos disponiveis.
                            },
                            "value": {
                                "description": "this is a value that matches anything"
                            }
                        },
                        "required": ["field", "operation", "value"]
                    }
                ]
            },
            "schema": {
                "type": "object"
            }
        },
        "required": [
            "resource",
            "page",
            "perpage",
            "filters",
            "schema"
        ]
    }


@dataclass
class UHQLRequestFilter:
    field: str
    op: str
    value: str


class UHQLRequest:
    def __init__(self, uhqlrequest: dict):
        resource: str
        page: int
        perpage: int
        filters: dict
        required: List[UHQLRequestFilter]

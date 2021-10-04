import yaml
import os


DEFAULT_API_RESP = {
    "description": "INFO or ERROR",
    "schema": {
        "properties": {"message": {"type": "string"}, "status": {"type": "integer"}},
        "type": "object",
    },
}


BULK_API_RESP_DATA = {
    "properties": {
        "errors": {
            "items": {
                "properties": {
                    "error_msg": {"type": "string"},
                    "key_pattern": {"type": "object"},
                    "key_value": {"type": "object"},
                },
                "type": "object",
            },
            "type": "array",
        },
        "processed": {"type": "integer"},
        "sent": {"type": "integer"},
    },
    "type": "object",
}


BULK_API_SUCCESS_RESP = {
    "description": "Returned if any of requests were successful",
    "schema": BULK_API_RESP_DATA,
}
BULK_API_FALIURE_RESP = {
    "description": "Returned if all of requests were unsuccessful",
    "schema": BULK_API_RESP_DATA,
}


GET_SINGLE_PARAMS = [
    {
        "description": "Item ID.",
        "in": "path",
        "name": "_id",
        "required": True,
        "type": "string",
    }
]


GET_ALL_PARAMS = [
    {
        "default": 20,
        "description": "The numbers of items to return.",
        "in": "query",
        "maximum": 100,
        "minimum": 1,
        "name": "limit",
        "required": False,
        "type": "integer",
    },
    {
        "default": 0,
        "description": "The number of items to skip before starting to collect the result set.",
        "in": "query",
        "minimum": 0,
        "name": "offset",
        "required": False,
        "type": "integer",
    },
    {
        "collectionFormat": "csv",
        "default": ["-_id"],
        "in": "query",
        "items": {"type": "string"},
        "name": "sort",
        "type": "array",
        "uniqueItems": True,
    },
    {
        "default": False,
        "description": "Switch for search case sensitivity setting",
        "in": "query",
        "name": "ignore_case",
        "required": False,
        "type": "boolean",
    },
    {
        "default": True,
        "description": "Switch for search whole word setting",
        "in": "query",
        "name": "whole_word",
        "required": False,
        "type": "boolean",
    },
    {
        "default": False,
        "description": "Force AND as filer items operator (default is OR)",
        "in": "query",
        "name": "force_and",
        "required": False,
        "type": "boolean",
    },
    {
        "collectionFormat": "csv",
        "in": "query",
        "items": {"type": "string"},
        "name": "filtering",
        "type": "array",
    },
    {
        "default": True,
        "description": "Switch mode for shape list - include only(True)/exclude_only(False)",
        "in": "query",
        "name": "shape_mode",
        "required": False,
        "type": "boolean",
    },
    {
        "collectionFormat": "csv",
        "in": "query",
        "items": {"type": "string"},
        "name": "shape",
        "type": "array",
    },
    {
        "in": "query",
        "items": {"type": "string"},
        "maxItems": 4,
        "minItems": 2,
        "name": "created_at",
        "type": "array",
    },
    {
        "in": "query",
        "items": {"type": "string"},
        "maxItems": 4,
        "minItems": 2,
        "name": "updated_at",
        "type": "array",
    },
]


def insert_paths_parts(paths):
    """
    pyyaml loose reference without:
    yaml.load(yaml.dump(GET_ALL_PARAMS))
    """
    for path, methods in paths.items():

        if "/bulk" in path:
            for method, definition in methods.items():
                if method in ["patch", "post"]:
                    if not definition.get("responses"):
                        definition["responses"] = {}
                        definition["responses"]["201"] = yaml.load(
                            yaml.dump(BULK_API_SUCCESS_RESP)
                        )
                        definition["responses"]["400"] = yaml.load(
                            yaml.dump(BULK_API_FALIURE_RESP)
                        )

        GET = methods.get("get", {})
        if "}" not in path and not GET.get("parameters"):
            GET["parameters"] = yaml.load(yaml.dump(GET_ALL_PARAMS))

        if "}" in path and not GET.get("parameters"):
            GET["parameters"] = yaml.load(yaml.dump(GET_SINGLE_PARAMS))
        DELETE = methods.get("delete", {})
        if "}" in path and not DELETE.get("parameters"):
            DELETE["parameters"] = yaml.load(yaml.dump(GET_SINGLE_PARAMS))

        for method, definition in methods.items():
            if not definition.get("produces"):
                definition["produces"] = ["application/json"]
            if method in ["patch", "post"]:
                if not definition.get("consumes"):
                    definition["consumes"] = ["application/json"]
            if not definition.get("responses", {}).get("default"):
                d = paths[path][method]
                if not d.get("responses"):
                    d["responses"] = {}
                d["responses"]["default"] = yaml.load(yaml.dump(DEFAULT_API_RESP))


def complete_yaml(yaml_file_path, api_version, host, base_path, scheme):
    with open(yaml_file_path) as f:
        y = yaml.load(f)
        insert_paths_parts(y.get("paths", {}))
        y["swagger"] = "2.0"
        y["host"] = host
        y["basePath"] = "%s/%s" % (base_path.replace("-VER-", api_version), os.path.basename(yaml_file_path).split(".")[0])
        y["schemes"] = [scheme]
        if y.get("info"):
            y["info"]["version"] = api_version
        y["securityDefinitions"] = {
            "API-Key": {"in": "header", "name": "X-API-Key", "type": "apiKey"}
        }
        return yaml.dump(y)

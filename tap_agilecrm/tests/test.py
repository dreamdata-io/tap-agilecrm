from tap_agilecrm import load_schema

from google.cloud import bigquery
from google.cloud.bigquery.job import SourceFormat
from google.cloud.bigquery import Dataset, WriteDisposition
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery import LoadJobConfig
from google.api_core import exceptions

# COPIED FROM github.com/realself
# https://github.com/RealSelf/target-bigquery/blob/master/target_bigquery.py#L54-L108
def define_schema(field, name):
    schema_name = name
    schema_type = "STRING"
    schema_mode = "NULLABLE"
    schema_description = None
    schema_fields = ()

    if "type" not in field and "anyOf" in field:
        for types in field["anyOf"]:
            if types["type"] == "null":
                schema_mode = "NULLABLE"
            else:
                field = types

    if isinstance(field["type"], list):
        if field["type"][0] == "null":
            schema_mode = "NULLABLE"
        else:
            schema_mode = "required"
        schema_type = field["type"][-1]
    else:
        schema_type = field["type"]
    if schema_type == "object":
        schema_type = "RECORD"
        schema_fields = tuple(build_schema(field))
    if schema_type == "array":
        schema_type = field.get("items").get("type")
        schema_mode = "REPEATED"
        if schema_type == "object":
            schema_type = "RECORD"
            schema_fields = tuple(build_schema(field.get("items")))

    if schema_type == "string":
        if "format" in field:
            if field["format"] == "date-time":
                schema_type = "timestamp"

    if schema_type == "number":
        schema_type = "FLOAT"

    return (schema_name, schema_type, schema_mode, schema_description, schema_fields)


def build_schema(schema):
    SCHEMA = []
    for key in schema["properties"].keys():

        if not (bool(schema["properties"][key])):
            # if we endup with an empty record.
            continue

        schema_name, schema_type, schema_mode, schema_description, schema_fields = define_schema(
            schema["properties"][key], key
        )
        if isinstance(schema_type, list):
            print(schema_name, schema_type, schema_fields)
        field = SchemaField(
            schema_name, schema_type, schema_mode, schema_description, schema_fields
        )
        import pprint

        pprint.pprint(field)
        SCHEMA.append(field)

    return SCHEMA


if __name__ == "__main__":
    for schema_name in ["contact", "deal", "company"]:
        schema = load_schema(schema_name)
        bs = build_schema(schema)
        import pprint

        pprint.pprint(bs)


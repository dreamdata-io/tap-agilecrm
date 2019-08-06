#!/usr/bin/env python

import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, Generator, List, Set

import singer
from singer import utils

from agilecrm_client import AgileCRM

REQUIRED_CONFIG_KEYS = ["api_key", "email", "domain"]

logger = singer.get_logger()

args = utils.parse_args(REQUIRED_CONFIG_KEYS)

EMAIL = args.config.get("email")
DOMAIN = args.config.get("domain")
API_KEY = args.config.get("api_key")

client = AgileCRM(EMAIL, DOMAIN, API_KEY)


def main():
    if args.discover:
        discover_stream = discover()
        print(json.dumps(discover_stream, sort_keys=True, indent="  "))
        return

    process_companies()
    process_contacts()
    process_deals()

def process_companies(**kwargs):
    process_stream(
        stream_name = "company_entity",
        stream_generator = client.list_companies(**kwargs),
        key_properties = ["id"],
        bookmark_properties = ["updated_time"],
    )

def process_contacts(**kwargs):
    process_stream(
        stream_name = "contact_entity",
        stream_generator = client.list_contacts(**kwargs),
        key_properties = ["id"],
        bookmark_properties = ["updated_time"],
    )

def process_deals(**kwargs):
    process_stream(
        stream_name = "deal", # no '_entity' suffix here
        stream_generator = client.list_deals(**kwargs),
        key_properties = ["id"],
        bookmark_properties = ["updated_time"],
    )

def process_stream(
    stream_name: str,
    stream_generator: Generator[Dict[str, Any], None, None],
    key_properties: List[str],
    bookmark_properties: str = None,
    stream_alias: str = None,
    include_fields: Set[str] = None,
):
    # load schema from disk
    schema = load_schema(stream_name)

    # write schema
    singer.write_schema(stream_name, schema, key_properties, stream_alias)

    # write records
    with singer.metrics.record_counter(stream_name or stream_alias) as counter:
        for record in stream_generator:
            if include_fields:
                keys = list(record.keys())
                for key in keys:
                    if key in include_fields:
                        continue
                    record.pop(key)

            # write record with timestamp
            singer.write_record(stream_name, record, time_extracted=utils.now())
            
            # instrument with metrics to allow consumers to receive progress
            counter.increment(1)


def load_schema(stream_name):
    with open(f"schemas/{stream_name}_schema.json", "r") as fp:
        return json.load(fp)

def write_record(record, include_fields):
    entity_type = record["entity_type"]
    with open(f"schemas/{entity_type}_payload.json", "w") as fp:
        json.dump(record, fp, sort_keys=True, indent="  ")
    
    keys = list(record.keys())
    for key in keys:
        if key in include_fields:
            continue
        record.pop(key)
    
    with open(f"schemas/{entity_type}_filtered.json", "w") as fp:
        json.dump(record, fp, sort_keys=True, indent="  ")

def discover():
    stream_names = ["company_entity", "contact_entity", "deal"]
    streams = [{"tap_stream_id": stream_name, "stream": stream_name, "schema": load_schema(stream_name)} for stream_name in stream_names]
    return {"streams": streams}
    

if __name__ == "__main__":
    main()

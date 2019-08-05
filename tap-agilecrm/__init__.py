#!/usr/bin/env python

import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, Generator, List

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
    process_companies()
    process_contacts()
    process_deals()

def process_companies(**kwargs):
    process_stream(
        stream_name = "company",
        stream_generator = client.list_companies(**kwargs),
        key_properties = ["id"],
        bookmark_properties = ["updated_time"],
    )

def process_contacts(**kwargs):
    process_stream(
        stream_name = "contact",
        stream_generator = client.list_contacts(**kwargs),
        key_properties = ["id"],
        bookmark_properties = ["updated_time"],
    )

def process_deals(**kwargs):
    process_stream(
        stream_name = "deal",
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
):
    # load schema from disk
    with open(f"schemas/{stream_name}_schema.json", "r") as fp:
        schema = json.load(fp)

    # write schema
    singer.write_schema(stream_name, schema, key_properties, stream_alias)

    # write records
    # instrument with metrics to allow consumers to receive progress
    with singer.metrics.record_counter(stream_name or stream_alias) as counter:
        for record in stream_generator:
            singer.write_record(stream_name, record, time_extracted=utils.now())
            counter.increment(1)

if __name__ == "__main__":
    main()

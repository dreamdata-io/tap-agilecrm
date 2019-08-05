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
    process_companies()
    process_contacts()
    process_deals()

def process_companies(**kwargs):
    fields = {
        "created_time",
        "entity_type",
        "company_entity",
        "id",
        "is_lead_converted",
        "klout_score",
        "last_called",
        "last_campaign_emaild",
        "last_contacted",
        "last_emailed",
        "lead_converted_time",
        "lead_score",
        "lead_source_id",
        "lead_status_id",
        "owner",
        "properties",
        "star_value",
        "tags",
        "trashed_time",
        "type",
        "updated_time",
        "viewed_time",
    }
    
    process_stream(
        stream_name = "company_entity",
        stream_generator = client.list_companies(**kwargs),
        key_properties = ["id"],
        bookmark_properties = ["updated_time"],
        include_fields=fields,
    )

def process_contacts(**kwargs):
    fields = {
        "contact_company_id",
        "created_time",
        "entity_type",
        "id",
        "is_lead_converted",
        "klout_score",
        "last_called",
        "last_campaign_emaild",
        "last_contacted",
        "last_emailed",
        "lead_converted_time",
        "lead_score",
        "lead_source_id",
        "lead_status_id",
        "owner",
        "properties",
        "source",
        "star_value",
        "tags",
        "type",
        "updated_time",
        "viewed_time",
    }
    process_stream(
        stream_name = "contact_entity",
        stream_generator = client.list_contacts(**kwargs),
        key_properties = ["id"],
        bookmark_properties = ["updated_time"],
        include_fields=fields,
    )

def process_deals(**kwargs):
    fields = {
        "apply_discount",
        "archived",
        "close_date",
        "colorName",
        "contact_ids",
        "contacts",
        "created_time",
        "currency_conversion_value",
        "custom_data",
        "deal_source_id",
        "discount_amt",
        "discount_type",
        "discount_value",
        "entity_type",
        "expected_value",
        "id",
        "isCurrencyUpdateRequired",
        "lost_reason_id",
        "milestone",
        "milestone_changed_time",
        "name",
        "pipeline_id",
        "probability",
        "products",
        "tags",
        "total_deal_value",
        "updated_time",
    }

    process_stream(
        stream_name = "deal", # no '_entity' suffix here
        stream_generator = client.list_deals(**kwargs),
        key_properties = ["id"],
        bookmark_properties = ["updated_time"],
        include_fields=fields,
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
    with open(f"schemas/{stream_name}_schema.json", "r") as fp:
        schema = json.load(fp)

    # write schema
    singer.write_schema(stream_name, schema, key_properties, stream_alias)

    # write records
    # instrument with metrics to allow consumers to receive progress
    with singer.metrics.record_counter(stream_name or stream_alias) as counter:
        for record in stream_generator:
            if include_fields:
                keys = list(record.keys())
                for key in keys:
                    if key in include_fields:
                        continue
                    record.pop(key)

            singer.write_record(stream_name, record, time_extracted=utils.now())

            counter.increment(1)


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


if __name__ == "__main__":
    main()

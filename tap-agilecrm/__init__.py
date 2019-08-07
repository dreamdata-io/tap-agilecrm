#!/usr/bin/env python

import json
import os
import sys
from datetime import datetime

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

    state = args.state

    streams = [process_companies, process_contacts, process_deals]

    for stream in streams:
        stream(state)
        singer.write_state(state)


def process_companies(state):
    stream_name = "company_entity"

    checkpoint = singer.get_bookmark(state, stream_name, "updated_time")

    logger.info(f"streaming {stream_name}: initiated")
    process_stream(
        state,
        stream_name=stream_name,
        stream_generator=client.list_companies_dynamic(checkpoint=checkpoint),
        key_properties=["id"],
        bookmark_properties=["updated_time"],
    )
    logger.info(f"streaming {stream_name}: done")


def process_contacts(state):
    stream_name = "contact_entity"

    checkpoint = singer.get_bookmark(state, stream_name, "updated_time")

    logger.info(f"streaming {stream_name}: initiated")
    process_stream(
        state,
        stream_name=stream_name,
        stream_generator=client.list_contacts_dynamic(checkpoint=checkpoint),
        key_properties=["id"],
        bookmark_properties=["updated_time"],
    )
    logger.info(f"streaming {stream_name}: done")


def process_deals(state):
    stream_name = "deal"  # no '_entity' suffix here

    logger.info(f"streaming {stream_name}: initiated")
    process_stream(
        state,
        stream_name=stream_name,
        stream_generator=client.list_deals(),
        key_properties=["id"],
        bookmark_properties=["updated_time"],
    )
    logger.info(f"streaming {stream_name}: done")


def process_stream(
    state,
    stream_name,
    stream_generator,
    key_properties,
    bookmark_properties=None,
    stream_alias=None,
    include_fields=None,
):
    # load schema from disk
    schema = load_schema(stream_name)

    # write schema
    singer.write_schema(stream_name, schema, key_properties, stream_alias)

    stream_state = singer.get_bookmark(state, stream_name, "updated_time")
    most_recent_update = stream_state or None
    previous_updated_time = None

    # write records
    with singer.metrics.record_counter(stream_name or stream_alias) as counter:
        i = 0
        for record in stream_generator:
            i += 1
            # ensure that created_time == updated_time
            # there are cases where updated_time is 0 if the record has never been updated
            updated_time = record["updated_time"]
            created_time = record["created_time"]
            record_id = record["id"]
            if updated_time == 0:
                # logger.info(f"added timestamp: {created_time}")
                record["updated_time"] = created_time

            # do not write records that are older than the most recent updated_time from the state
            if stream_state and stream_state > updated_time:
                continue

            # remove fields that are not in the include_fields
            if include_fields:
                keys = list(record.keys())
                for key in keys:
                    if key in include_fields:
                        continue
                    record.pop(key)

            # only applicable in the first iteration
            if not previous_updated_time:
                previous_updated_time = updated_time

            # alert when item is out of order
            if previous_updated_time and previous_updated_time > updated_time:
                logger.warning(
                    f"{i}: item id={record_id} out of order updated_time={updated_time}"
                )
            # only update previous_update_time when it is in-order
            else:
                previous_updated_time = updated_time

            # write record with timestamp
            singer.write_record(stream_name, record, time_extracted=utils.now())
            
            # instrument with metrics to allow consumers to receive progress
            counter.increment(1)

            # only relevant for the first iteration
            if not most_recent_update:
                most_recent_update = updated_time
            elif most_recent_update < updated_time:
                most_recent_update = updated_time

    return singer.write_bookmark(state, stream_name, "updated_time", most_recent_update)

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

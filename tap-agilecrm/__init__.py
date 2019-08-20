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

# optional configuration options
CONFIG = args.config.get("config")

client = AgileCRM(EMAIL, DOMAIN, API_KEY)


def main():
    if args.discover:
        discover_stream = discover()
        print(json.dumps(discover_stream, sort_keys=True, indent="  "))
        return

    state = args.state

    streams = {
        "contact": process_contacts,
        "company": process_companies,
        "deal": process_deals,
    }

    for stream_name in CONFIG.keys():
        if stream_name not in streams:
            logger.error(
                f"stream name: '{stream_name}' is unsupported. Supported streams: {streams.keys()}"
            )
            sys.exit(1)

        stream_processor = streams[stream_name]
        stream_config = CONFIG.get(stream_name, {})

        stream_processor(state, stream_config)


def process_companies(state, config):
    stream_name = "company"

    checkpoint = singer.get_bookmark(state, stream_name, "updated_time")
    exclude_fields = config.get("exclude_fields")

    logger.info(f"streaming {stream_name}: initiated")
    process_stream(
        state,
        stream_name=stream_name,
        stream_generator=client.list_companies_dynamic(checkpoint=checkpoint),
        key_properties=["id"],
        bookmark_properties=["updated_time"],
        exclude_fields=exclude_fields,
    )
    logger.info(f"streaming {stream_name}: done")


def process_contacts(state, config):
    stream_name = "contact"

    checkpoint = singer.get_bookmark(state, stream_name, "updated_time")
    exclude_fields = config.get("exclude_fields")

    logger.info(f"streaming {stream_name}: initiated")
    process_stream(
        state,
        stream_name=stream_name,
        stream_generator=client.list_contacts_dynamic(checkpoint=checkpoint),
        key_properties=["id"],
        bookmark_properties=["updated_time"],
        exclude_fields=exclude_fields,
    )
    logger.info(f"streaming {stream_name}: done")


def process_deals(state, config):
    stream_name = "deal"

    exclude_fields = config.get("exclude_fields")

    logger.info(f"streaming {stream_name}: initiated")
    process_stream(
        state,
        stream_name=stream_name,
        stream_generator=client.list_deals(),
        key_properties=["id"],
        bookmark_properties=["updated_time"],
        exclude_fields=exclude_fields,
    )
    logger.info(f"streaming {stream_name}: done")


def process_stream(
    state,
    stream_name,
    stream_generator,
    key_properties,
    bookmark_properties=None,
    stream_alias=None,
    exclude_fields=None,
):
    # load schema from disk
    schema = load_schema(stream_name)

    # write schema
    singer.write_schema(stream_name, schema, key_properties, stream_alias)

    stream_state = singer.get_bookmark(state, stream_name, "updated_time")
    last_updated_time = None

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

            # do not write records that are older than the most recent updated_time
            # from the optional state
            if stream_state and stream_state > updated_time:
                continue

            # remove fields that are in the exclude_fields argument
            if exclude_fields:
                for field in exclude_fields:
                    record.pop(field, None)

            # write warning if the current record is newer then the previous record
            if last_updated_time and last_updated_time < updated_time:
                logger.warning(
                    f"{i}: item id={record_id} out of order updated_time={updated_time}"
                )

            last_updated_time = updated_time

            # write record with extracted timestamp
            singer.write_record(stream_name, record, time_extracted=utils.now())

            # instrument with metrics to allow targets to receive progress
            counter.increment(1)

    return singer.write_bookmark(state, stream_name, "updated_time", last_updated_time)


def load_schema(stream_name):
    with open(f"schemas/{stream_name}_schema.json", "r") as fp:
        return json.load(fp)


def discover():
    stream_names = ["company_entity", "contact_entity", "deal"]
    streams = [
        {
            "tap_stream_id": stream_name,
            "stream": stream_name,
            "schema": load_schema(stream_name),
        }
        for stream_name in stream_names
    ]
    return {"streams": streams}


if __name__ == "__main__":
    main()

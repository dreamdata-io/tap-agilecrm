#!/usr/bin/env python

import json
import os
import sys
from datetime import datetime

import singer
from singer import utils

from agilecrm_client import AgileCRM

logger = singer.get_logger()

args = utils.parse_args(["config"])

AGILECRM_EMAIL = "AGILECRM_EMAIL"
AGILECRM_DOMAIN = "AGILECRM_DOMAIN"
AGILECRM_API_KEY = "AGILECRM_API_KEY"

EMAIL = args.config.get("email") or os.environ.get(AGILECRM_EMAIL)
DOMAIN = args.config.get("domain") or os.environ.get(AGILECRM_DOMAIN)
API_KEY = args.config.get("api_key") or os.environ.get(AGILECRM_API_KEY)

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

    for stream_name in sorted(CONFIG.keys()):
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

    logger.info(f"streaming {stream_name}: initiated")

    checkpoint = singer.get_bookmark(state, stream_name, "updated_time")
    if checkpoint:
        logger.info(f"- previous state: {checkpoint}")

    exclude_fields = config.get("exclude_fields")
    if exclude_fields:
        logger.info(f"- ignoring fields: {exclude_fields}")
    
    sample_size = config.get("sample_size")
    if sample_size:
        logger.info(f"- sample_size: {sample_size}")

    process_stream(
        state,
        stream_name=stream_name,
        stream_generator=client.list_companies_dynamic(
            checkpoint=checkpoint, global_sort_key="updated_time"
        ),
        key_properties=["id"],
        bookmark_properties=["updated_time"],
        exclude_fields=exclude_fields,
        sample_size=sample_size,
    )
    logger.info(f"emitting state: {state}")

    singer.write_state(state)

    logger.info(f"streaming {stream_name}: done")


def process_contacts(state, config):
    stream_name = "contact"

    logger.info(f"streaming {stream_name}: initiated")

    checkpoint = singer.get_bookmark(state, stream_name, "updated_time")
    if checkpoint:
        logger.info(f"- previous state: {checkpoint}")

    exclude_fields = config.get("exclude_fields")
    if exclude_fields:
        logger.info(f"- ignoring fields: {exclude_fields}")
    
    sample_size = config.get("sample_size")
    if sample_size:
        logger.info(f"- sample_size: {sample_size}")


    process_stream(
        state,
        stream_name=stream_name,
        stream_generator=client.list_contacts_dynamic(
            checkpoint=checkpoint, global_sort_key="updated_time"
        ),
        key_properties=["id"],
        bookmark_properties=["updated_time"],
        exclude_fields=exclude_fields,
        sample_size=sample_size,
    )

    logger.info(f"emitting state: {state}")

    singer.write_state(state)

    logger.info(f"streaming {stream_name}: done")


def process_deals(state, config):
    stream_name = "deal"

    logger.info(f"streaming {stream_name}: initiated")

    checkpoint = singer.get_bookmark(state, stream_name, "updated_time")
    if checkpoint:
        logger.info(f"- previous state: {checkpoint}")

    exclude_fields = config.get("exclude_fields")
    if exclude_fields:
        logger.info(f"- ignoring fields: {exclude_fields}")
    
    sample_size = config.get("sample_size")
    if sample_size:
        logger.info(f"- sample_size: {sample_size}")

    process_stream(
        state,
        stream_name=stream_name,
        stream_generator=client.list_deals(),
        key_properties=["id"],
        bookmark_properties=["updated_time"],
        exclude_fields=exclude_fields,
        sample_size=sample_size,
    )
    logger.info(f"emitting state: {state}")

    singer.write_state(state)

    logger.info(f"streaming {stream_name}: done")


def process_stream(
    state,
    stream_name,
    stream_generator,
    key_properties,
    bookmark_properties=None,
    stream_alias=None,
    exclude_fields=None,
    sample_size=None,
):
    # load schema from disk
    schema = load_schema(stream_name)

    # write schema
    singer.write_schema(stream_name, schema, key_properties, stream_alias)

    stream_state = singer.get_bookmark(state, stream_name, "updated_time")
    most_recent_update = stream_state or 0

    # write records
    try:
        with singer.metrics.record_counter(stream_name or stream_alias) as counter:
            i = 0
            for record in stream_generator:
                i += 1
                updated_time = record["updated_time"]
                created_time = record["created_time"]

                # ensure that created_time == updated_time
                # there are cases where updated_time is 0
                # assumingly if the record has never been updated
                if updated_time == 0:
                    record["updated_time"] = created_time

                # do not write records that are older than the most recent updated_time
                # from the optional state
                if stream_state and stream_state > updated_time and stream_state > created_time:
                    continue

                # remove fields that are in the exclude_fields argument
                if exclude_fields:
                    for field in exclude_fields:
                        record.pop(field, None)


                # write record with extracted timestamp
                singer.write_record(stream_name, record, time_extracted=utils.now())

                # applies to the first iteration
                if most_recent_update < updated_time:
                    most_recent_update = updated_time

                # instrument with metrics to allow targets to receive progress
                counter.increment(1)

                if sample_size and sample_size < i:
                    break

    except Exception as err:
        logger.error(f"{str(err)}")
    finally:
        if not stream_state:
            return singer.write_bookmark(
                state, stream_name, "updated_time", most_recent_update
            )

        return singer.write_bookmark(
            state,
            stream_name,
            "updated_time",
            most_recent_update if most_recent_update > stream_state else stream_state,
        )


def load_schema(stream_name):
    with open(f"schemas/{stream_name}_schema_infer.json", "r") as fp:
        return json.load(fp)


def discover():
    stream_names = ["company", "contact", "deal"]
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

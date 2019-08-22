#!/usr/bin/env python

import json
import os
import sys
from datetime import datetime

import singer
from singer import utils

from tap_agilecrm.client import AgileCRM

logger = singer.get_logger()

args = utils.parse_args(["config"])

AGILECRM_EMAIL = "AGILECRM_EMAIL"
AGILECRM_DOMAIN = "AGILECRM_DOMAIN"
AGILECRM_API_KEY = "AGILECRM_API_KEY"

EMAIL = args.config.get("email") or os.environ.get(AGILECRM_EMAIL)
DOMAIN = args.config.get("domain") or os.environ.get(AGILECRM_DOMAIN)
API_KEY = args.config.get("api_key") or os.environ.get(AGILECRM_API_KEY)

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

    bookmark_property = "updated_time"

    # optional configuration options
    config = args.config.get("config", {})
    if not config:
        logger.error(
            "no streams configured for this tap - provide a 'config' field in the configurations object"
        )
        sys.exit(1)

    for stream_name, stream_config in sorted(config.items()):
        if stream_name not in streams:
            logger.error(
                f"stream name: '{stream_name}' is unsupported. Supported streams: {streams.keys()}"
            )
            sys.exit(1)

        stream_handler = streams[stream_name]

        logger.info(f"[{stream_name}] streaming..")

        checkpoint = singer.get_bookmark(state, stream_name, bookmark_property)
        if checkpoint:
            logger.info(f"[{stream_name}] previous state: {checkpoint}")

        exclude_fields = stream_config.get("exclude_fields")
        if exclude_fields:
            logger.info(f"[{stream_name}] ignoring fields: {exclude_fields}")

        sample_size = stream_config.get("sample_size")
        if sample_size:
            logger.info(f"[{stream_name}] sample_size: {sample_size}")

        new_checkpoint = stream_handler(
            checkpoint=checkpoint,
            exclude_fields=exclude_fields,
            sample_size=sample_size,
        )
        singer.write_bookmark(state, stream_name, bookmark_property, new_checkpoint)
        logger.info(f"[{stream_name}] emitting state: {state}")
        singer.write_state(state)
        logger.info(f"[{stream_name}] done")


def process_companies(checkpoint=None, exclude_fields=None, sample_size=None):
    stream_name = "company"

    return process_stream(
        stream_name=stream_name,
        stream_generator=client.list_companies_dynamic(
            checkpoint=checkpoint, global_sort_key="updated_time"
        ),
        key_properties=["id"],
        checkpoint=checkpoint,
        exclude_fields=exclude_fields,
        sample_size=sample_size,
    )


def process_contacts(checkpoint=None, exclude_fields=None, sample_size=None):
    stream_name = "contact"

    return process_stream(
        stream_name=stream_name,
        stream_generator=client.list_contacts_dynamic(
            checkpoint=checkpoint, global_sort_key="updated_time"
        ),
        key_properties=["id"],
        checkpoint=checkpoint,
        exclude_fields=exclude_fields,
        sample_size=sample_size,
    )


def process_deals(checkpoint=None, exclude_fields=None, sample_size=None):
    stream_name = "deal"

    return process_stream(
        stream_name=stream_name,
        stream_generator=client.list_deals(),
        key_properties=["id"],
        checkpoint=checkpoint,
        exclude_fields=exclude_fields,
        sample_size=sample_size,
    )


def process_stream(
    stream_name,
    stream_generator,
    key_properties,
    checkpoint=None,
    exclude_fields=None,
    sample_size=None,
):
    # load schema from disk
    schema = load_schema(stream_name)

    # write schema
    singer.write_schema(stream_name, schema, key_properties)

    stream_state = checkpoint
    most_recent_update = stream_state or 0

    # write records
    try:
        with singer.metrics.record_counter(stream_name) as counter:
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
                if (
                    stream_state
                    and stream_state > updated_time
                    and stream_state > created_time
                ):
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
            return most_recent_update

        return (
            most_recent_update if most_recent_update > stream_state else stream_state,
        )


def load_schema(stream_name):
    with open(f"tap_agilecrm/schemas/{stream_name}_schema_infer.json", "r") as fp:
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

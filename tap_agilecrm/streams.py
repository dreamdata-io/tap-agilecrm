#!/usr/bin/env python

import json
import sys
from typing import Dict, Any, Optional, Callable

import singer
from singer import utils

from tap_agilecrm.client import AgileCRM

logger = singer.get_logger()


def process_streams(client: AgileCRM, config: Dict[str, Any], state: Dict[str, Any]):
    bookmark_property = "updated_time"

    supported_streams = {
        "contact": client.list_contacts_dynamic,
        "company": client.list_companies_dynamic,
        "deal": client.list_deals,
    }

    for stream_name, stream_config in sorted(config.items()):
        if stream_name not in supported_streams:
            logger.error(
                f"stream: '{stream_name}' is not in list of supported streams: {supported_streams.keys()}"
            )
            sys.exit(1)

        generator = supported_streams[stream_name]
        process_stream(stream_name, generator, bookmark_property, stream_config, state)


def process_stream(
    stream_name: str,
    stream_generator,
    bookmark_property: str,
    config: Dict[str, Any],
    state: Dict[str, Any],
):
    logger.info(f"[{stream_name}] streaming..")

    checkpoint = singer.get_bookmark(state, stream_name, bookmark_property)
    if checkpoint:
        logger.info(f"[{stream_name}] previous state: {checkpoint}")

    exclude_fields = config.get("exclude_fields")
    if exclude_fields:
        logger.info(f"[{stream_name}] ignoring fields: {exclude_fields}")

    sample_size = config.get("sample_size")
    if sample_size:
        logger.info(f"[{stream_name}] sample_size: {sample_size}")

    new_checkpoint = emit_stream(
        stream_name, stream_generator, checkpoint, exclude_fields, sample_size
    )

    singer.write_bookmark(state, stream_name, bookmark_property, new_checkpoint)

    logger.info(f"[{stream_name}] emitting state: {state}")

    singer.write_state(state)

    logger.info(f"[{stream_name}] done")


def emit_stream(stream_name, stream_generator, checkpoint, exclude_fields, sample_size):
    # load schema from disk
    schema = load_schema(stream_name)

    # write schema
    key_properties = ["id"]
    singer.write_schema(stream_name, schema, key_properties)

    stream_state = checkpoint
    most_recent_update = stream_state or 0

    i = 0
    try:
        with singer.metrics.record_counter(stream_name) as counter:
            for record in stream_generator(global_sort_key="updated_time"):
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

                if sample_size and sample_size <= i:
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

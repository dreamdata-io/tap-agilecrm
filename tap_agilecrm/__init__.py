#!/usr/bin/env python

import json
import os
import sys
from datetime import datetime
from typing import Dict, Any, Optional, Callable

import singer
from singer import utils

from tap_agilecrm.client import AgileCRM
from tap_agilecrm.streams import load_schema, process_streams

logger = singer.get_logger()


AGILECRM_EMAIL = "AGILECRM_EMAIL"
AGILECRM_DOMAIN = "AGILECRM_DOMAIN"
AGILECRM_API_KEY = "AGILECRM_API_KEY"


def main():
    args = utils.parse_args(["config"])
    EMAIL = args.config.get("email") or os.environ.get(AGILECRM_EMAIL)
    DOMAIN = args.config.get("domain") or os.environ.get(AGILECRM_DOMAIN)
    API_KEY = args.config.get("api_key") or os.environ.get(AGILECRM_API_KEY)

    client = AgileCRM(EMAIL, DOMAIN, API_KEY)

    if args.discover:
        discover_stream = discover()
        print(json.dumps(discover_stream, sort_keys=True, indent="  "))
        return

    state = args.state
    config = args.config.get("config", {})

    # optional configuration options
    if not config:
        logger.error(
            "no streams configured for this tap - provide a 'config' field in the configurations object"
        )
        sys.exit(1)

    process_streams(client, config, state)


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

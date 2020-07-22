#!/usr/bin/env python

import json
import sys
from datetime import datetime
from typing import Dict, Any, Optional, Callable

import singer
from singer import utils

from tap_agilecrm.client import AgileCRM
from tap_agilecrm.streams import process_streams

logger = singer.get_logger()


def main():
    args = utils.parse_args(["config"])
    EMAIL = args.config.get("email")
    DOMAIN = args.config.get("domain")
    API_KEY = args.config.get("api_key")

    client = AgileCRM(EMAIL, DOMAIN, API_KEY)

    state = args.state
    config = args.config.get("config", {})

    # optional configuration options
    if not config:
        logger.error(
            "no streams configured for this tap - provide a 'config' field in the configurations object"
        )
        sys.exit(1)

    process_streams(client, config, state)


if __name__ == "__main__":
    main()

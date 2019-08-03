#!/usr/bin/env python

import singer
from singer.utils import parse_args

from agilecrm_client import AgileCRM

REQUIRED_CONFIG_KEYS = ["api_key", "email", "domain"]


def main():
    args = parse_args(REQUIRED_CONFIG_KEYS)

    email = args.config.get("email")
    domain = args.config.get("domain")
    api_key = args.config.get("api_key")

    client = AgileCRM(email, domain, api_key)

    for company in client.list_companies(page_size=1):
        print(f"company: {company}")
        break

    for contact in client.list_contacts(page_size=1):
        print(f"contact: {contact}")
        break

    for deals in client.list_deals(page_size=1):
        print(f"deals: {deals}")
        break


if __name__ == "__main__":
    main()

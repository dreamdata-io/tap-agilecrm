from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import json


class AgileCRM:
    def __init__(
        self,
        email,
        domain,
        api_key,
        pagination_page_size=50,
        pagination_global_sort_key="updated_time",
        **kwargs,
    ):
        self.__session = Session(**kwargs)
        self.__session.auth = (email, api_key)
        self.__session.headers.update({"Accept": "application/json"})
        self.__session.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    total=5,
                    backoff_factor=5,
                    status_forcelist=[500, 502, 503, 504],
                    respect_retry_after_header=True,
                )
            ),
        )

        self.pagination_page_size = pagination_page_size
        self.pagination_global_sort_key = pagination_global_sort_key

        self.base_url = f"https://{domain}.agilecrm.com/dev/api/"

    def list_deals(self, page_size=None, global_sort_key=None, **kwargs):
        args = {
            "page_size": page_size or self.pagination_page_size,
            "global_sort_key": global_sort_key or self.pagination_global_sort_key,
        }
        yield from self.__paginate("GET", "opportunity", params=args, **kwargs)

    def list_companies_dynamic(
        self, page_size=None, global_sort_key=None, checkpoint=None, **kwargs
    ):
        filterJson = {"contact_type": "COMPANY"}
        if checkpoint:
            filterJson["rules"] = [
                {"LHS": "updated_time", "CONDITION": "IS BEFORE", "RHS": checkpoint}
            ]

        args = {
            "page_size": page_size or self.pagination_page_size,
            "global_sort_key": global_sort_key or self.pagination_global_sort_key,
            "filterJson": json.dumps(filterJson),
        }
        yield from self.__paginate(
            "POST", "filters/filter/dynamic-filter", data=args, **kwargs
        )

    def list_contacts_dynamic(
        self, page_size=None, global_sort_key=None, checkpoint=None, **kwargs
    ):
        filterJson = {"contact_type": "PERSON"}
        if checkpoint:
            filterJson["rules"] = [
                {"LHS": "updated_time", "CONDITION": "IS AFTER", "RHS": checkpoint}
            ]

        args = {
            "page_size": page_size or self.pagination_page_size,
            "global_sort_key": global_sort_key or self.pagination_global_sort_key,
            "filterJson": json.dumps(filterJson),
        }
        yield from self.__paginate(
            "POST", "filters/filter/dynamic-filter", data=args, **kwargs
        )

    def __paginate(self, method, path, data=None, params=None, **kwargs):
        """paginates over items with a trailing item containing a cursor and passes arguments as either 'data' or 'params' depending on method"""
        while True:
            items = self.request(method, path, data=data, params=params, **kwargs)

            if not items:
                return

            # remove possible 'cursor' value from last item
            cursor = items[-1].pop("cursor", None)

            for item in items:
                yield item

            if not cursor:
                return

            if data:
                data["cursor"] = cursor
            elif params:
                params["cursor"] = cursor

    def request(self, method, path, **kwargs):
        resp = self.__session.request(method, self.base_url + path, **kwargs)
        if resp:
            return resp.json()

        resp.raise_for_status()

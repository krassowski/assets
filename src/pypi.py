# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

"""Extension manager using pip as package manager and PyPi.org as packages source."""
import json
import pathlib
import re
import xmlrpc.client
from itertools import groupby
from time import sleep
from typing import Any, Callable, List, Optional, Tuple

import tornado
from jupyterlab.extensions.manager import ExtensionManager
from traitlets import CFloat, Unicode, config
from traitlets.config import LoggingConfigurable


def _fetch_package_metadata(name: str, latest_version: str, base_url: str) -> dict:
    http_client = tornado.httpclient.HTTPClient()
    response = http_client.fetch(
        base_url + f"/{name}/{latest_version}/json",
        headers={"Content-Type": "application/json"},
    )
    data = json.loads(response.body).get("info")

    # Keep minimal information to limit cache size
    return {
        k: data.get(k)
        for k in [
            "author",
            "bugtrack_url",
            "docs_url",
            "home_page",
            "license",
            "package_url",
            "project_url",
            "project_urls",
            "summary",
        ]
    }


PYPISTATS_ENDPOINT = "https://pypistats.org/api/packages/{package}/recent?period=month"


class PyPILabExtensions(LoggingConfigurable):
    """Extension manager using pip as package manager and PyPi.org as packages source."""

    base_url = Unicode("https://pypi.org/pypi", config=True, help="The base URL of PyPI index.")

    rpc_request_throttling = CFloat(
        1.0,
        config=True,
        help="Throttling time in seconds between PyPI requests using the XML-RPC API.",
    )

    def __init__(
        self,
        config = None,
        parent: Optional[config.Configurable] = None,
    ) -> None:
        super(PyPILabExtensions, self).__init__()
        # Combine XML RPC API and JSON API to reduce throttling by PyPI.org
        self._http_client = tornado.httpclient.AsyncHTTPClient()
        self._rpc_client = xmlrpc.client.ServerProxy(self.base_url)

        self.log.debug(f"Extensions list will be fetched from {self.base_url}.")

    def __throttleRequest(self, recursive: bool, fn: Callable, *args) -> Any:
        """Throttle XMLRPC API request

        Args:
            recursive: Whether to call the throttling recursively once or not.
            fn: API method to call
            *args: API method arguments
        Returns:
            Result of the method
        Raises:
            xmlrpc.client.Fault
        """
        try:
            data = fn(*args)
        except xmlrpc.client.Fault as err:
            if err.faultCode == -32500 and err.faultString.startswith("HTTPTooManyRequests:"):
                delay = 1.01
                match = re.search(r"Limit may reset in (\d+) seconds.", err.faultString)
                if match is not None:
                    delay = int(match.group(1) or "1")
                self.log.info(
                    f"HTTPTooManyRequests - Perform next call to PyPI XMLRPC API in {delay}s."
                )
                sleep(delay * self.rpc_request_throttling + 0.01)
                if recursive:
                    data = self.__throttleRequest(False, fn, *args)
                else:
                    data = fn(*args)

        return data

    def list_packages(
        self
    ) -> List[dict]:
        """List the available extensions.

        Note:
            This will list the packages based on the classifier
                Framework :: Jupyter :: JupyterLab :: Extensions :: Prebuilt

            We do not try to check if they are compatible (version wise)

        Returns:
            The available extensions in a mapping {name: metadata}
        """
        matches = self.__get_all_extensions()

        extensions = {}
        
        for name, group in groupby(matches, lambda e: e[0]):

            _, latest_version = list(group)[-1]
            data = _fetch_package_metadata(name, latest_version, self.base_url)

            normalized_name = self._normalize_name(name)

            package_urls = data.get("project_urls") or {}

            source_url = package_urls.get("Source Code")
            homepage_url = data.get("home_page") or package_urls.get("Homepage")
            documentation_url = data.get("docs_url") or package_urls.get("Documentation")
            bug_tracker_url = data.get("bugtrack_url") or package_urls.get("Bug Tracker")

            best_guess_home_url = (
                homepage_url
                or data.get("project_url")
                or data.get("package_url")
                or documentation_url
                or source_url
                or bug_tracker_url
            )

            extensions[normalized_name] = dict(
                name=normalized_name,
                description=data.get("summary"),
                homepage_url=best_guess_home_url,
                author=data.get("author"),
                license=data.get("license"),
                latest_version=ExtensionManager.get_semver_version(latest_version),
                pkg_type="prebuilt",
                bug_tracker_url=bug_tracker_url,
                documentation_url=documentation_url,
                package_manager_url=data.get("package_url"),
                repository_url=source_url,
                monthly_download=self._get_monthly_download(name)
            )

        return list(sorted(extensions.values(), key=lambda p: p.get("monthly_download", 0)))

    def __get_all_extensions(self) -> List[Tuple[str, str]]:
        self.log.debug("Requesting PyPI.org RPC API for prebuilt JupyterLab extensions.")
        return self.__throttleRequest(
            True,
            self._rpc_client.browse,
            ["Framework :: Jupyter :: JupyterLab :: Extensions :: Prebuilt"],
        )

    def _get_monthly_download(self, name: str) -> int:
        client = tornado.httpclient.HTTPClient()
        try:
            r = client.fetch(PYPISTATS_ENDPOINT.format(package=name))
            return json.loads(r.body).get("data", {}).get("last_month", 0)
        except BaseException as e:
            self.log.debug(f"Failed to get PyPI statistics for package '{name}'.")
            return 0

    def _normalize_name(self, name: str) -> str:
        """Normalize extension name.

        Remove `@` from npm scope and replace `/` and `_` by `-`.

        Args:
            name: Extension name
        Returns:
            Normalized name
        """
        return name.replace("@", "").replace("/", "-").replace("_", "-")


if __name__ == "__main__":
    store = pathlib.Path("pypi-jlab-extensions.json")
    with store.open("w", encoding="utf-8") as s:
        json.dump(PyPILabExtensions().list_packages(), s)

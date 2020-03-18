import os
from configparser import ConfigParser
from os import path
from datadog_export.logger import log

from datadog import initialize

allowed_properties = {"api_key", "app_key", "proxies", "api_host", "cacert", "mute"}


def read(section: str = "DEFAULT"):
    """
    reads the ~/.datadog.ini `section` with the following allowed properties

    :param section identifying a specific datadog account


    api_key: Datadog API key
    type api_key: string

    app_key: Datadog application key
    type app_key: string

    proxies: Proxy to use to connect to Datadog API
    type proxies: dictionary mapping protocol to the URL of the proxy.

    api_host: Datadog API endpoint
    type api_host: url

    cacert: Path to local certificate file used to verify SSL \
        certificates. Can also be set to True (default) to use the systems \
        certificate store, or False to skip SSL verification
    type cacert: path or boolean

    mute: Mute any ApiError or ClientError before they escape \
        from datadog.api.HTTPClient (default: True).
    type mute: boolean
    """
    parser = ConfigParser()
    parser.read(path.expanduser("~/.datadog.ini"))
    result = {k: v for (k, v) in parser.items(section) if k in allowed_properties}
    if not result.get("api_key"):
        result["api_key"] = os.environ.get(
            "DATADOG_API_KEY", os.environ.get("DD_API_KEY")
        )
    if not result.get("app_key"):
        result["app_key"] = os.environ.get(
            "DATADOG_APP_KEY", os.environ.get("DD_APP_KEY")
        )
    return result


def connect(section: str = "DEFAULT") -> dict:
    kwargs = read(section)

    if kwargs.get("api_key") and kwargs.get("app_key"):
        initialize(**kwargs)
    else:
        log.error(
            f"api_key/app_key missing from the environment and ~/.datadog.ini in the section {section}"
        )
        exit(1)
    return kwargs


def get_headers(section: str = "DEFAULT") -> dict:
    result = {}
    configuration = read(section)
    for name, header in [("api_key", "DD-API-KEY"), ("app_key", "DD-APPLICATION-KEY")]:
        if configuration.get(name):
            result[header] = configuration.get(name)

    return result

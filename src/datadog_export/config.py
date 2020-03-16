import os
from configparser import ConfigParser
from os import path

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
    return {k: v for (k, v) in parser.items(section) if k in allowed_properties}

os.environ

def connect(section: str = "DEFAULT") -> dict:
    kwargs = read(section)
    if (
        "api_key" in kwargs
        or os.environ.get("DATADOG_API_KEY", os.environ.get("DD_API_KEY"))
    ) and (
        "app_key" in kwargs
        or os.environ.get("DATADOG_APP_KEY", os.environ.get("DD_APP_KEY"))
    ):
        initialize(**kwargs)
    else:
        sys.stderr.write(
            f"ERROR: api_key/app_key missing from ~/.datadog.ini in the section {section}\n"
        )
        sys.exit(1)
    return kwargs


def get_headers(section: str = "DEFAULT") -> dict:
    result = {}
    configuration = read(section)
    for name, header in [("api_key", "DD-API-KEY"), ("app_key", "DD-APPLICATION-KEY")]:
        if configuration.get(name):
            result[header] = configuration.get(name)

    return result

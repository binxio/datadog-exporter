import json
import logging
import re
from copy import deepcopy
from datetime import datetime, timedelta

import click
import click_argument_types
import pytz
import requests
from datadog_export.config import connect, get_headers
from durations import Duration


class RateLimit(object):
    def __init__(self, headers: dict):
        self.limit = int(headers["X-RateLimit-Limit"])
        self.remaining = int(headers["X-RateLimit-Remaining"])
        self.period = int(headers["X-RateLimit-Period"])
        self.reset = int(headers["X-RateLimit-Reset"])


class Exporter(object):
    def __init__(self):
        self.account: str = "DEFAULT"
        self.window: Duration = Duration("60m")
        self._end_time: datetime = datetime.now().astimezone(pytz.UTC)
        self._start_time: datetime = self._end_time - timedelta(seconds=3600)
        self.metrics = []

    def connect(self):
        connect(self.account)

    @property
    def start_time(self) -> datetime:
        return self._start_time

    @start_time.setter
    def start_time(self, start_time):
        self._start_time = start_time.astimezone(pytz.UTC)

    @property
    def end_time(self) -> datetime:
        return self._end_time

    @end_time.setter
    def end_time(self, end_time):
        self._end_time = end_time.astimezone(pytz.UTC)

    def get_metrics(self):
        self.metrics = []
        headers = get_headers(self.account)
        response = requests.get(
            "https://api.datadoghq.com/api/v1/metrics",
            headers=headers,
            params={"from": int(self.start_time.timestamp())},
        )
        if response.status_code == 200:
            self.metrics = response.json()["metrics"]
        else:
            logging.error("failed to retrieve metrics, %s", response.text)
            exit(1)

    def export_rate_limit_exceeded(self, response, query):
        rate_limit = RateLimit(response.headers)
        logging.error(
            "rate limit exceeded of %s calls to /api/v1/query in %ss, retry in %ss",
            rate_limit.limit,
            rate_limit.period,
            rate_limit.reset,
        )
        logging.info('datadog-export metrics --start-time %s --end-time %s --window "%s" "%s"',
                            self.start_time.isoformat(), self.end_time.isoformat(), self.window.representation, query)

    def _get(self, st:datetime, et: datetime, query:str) -> requests.Response:
        return requests.get(
            "https://api.datadoghq.com/api/v1/query",
            headers=get_headers(self.account),
            params={
                "from": int(st.timestamp()),
                "to": int(et.timestamp()),
                "query": query
            },
        )


    @staticmethod
    def to_datetime(ts: float) -> datetime:
        result = datetime.fromtimestamp(ts/1000)
        result = result + timedelta(microseconds=ts % 1000)
        return result

    def convert_to_timestamps(self, response):
        r = deepcopy(response)
        if 'from_date' in r:
            r["from_date"] = self.to_datetime(r["from_date"]).isoformat()
            r["to_date"] = self.to_datetime(r["to_date"]).isoformat()
        for s in r["series"]:
            s["start"] =  self.to_datetime(s["start"]).isoformat()
            s["end"] =  self.to_datetime(s["end"]).isoformat()
            for point in s["pointlist"]:
                point[0] = self.to_datetime(point[0]).isoformat()
        return r

    def export(self, query):

        logging.info(f'exporting from {self.start_time} to {self.end_time} in {self.window} steps')
        st = self.start_time
        while st < self.end_time:
            et = st + timedelta(seconds=self.window.to_seconds())
            headers = get_headers(self.account)
            response = self._get(st, et, query)
            if response.status_code == 200:
                r = self.convert_to_timestamps(response.json())
                if r["status"] != "error":
                    print(json.dumps(r, indent=2))
                    st = st + timedelta(seconds=self.window.to_seconds())
                else:
                    logging.error(r["error"])
                    exit(1)

            elif response.status_code != 429:
                logging.error(
                    "%s for %s returned %s, %s",
                    response.request.url,
                    query,
                    response.status_code,
                    response.text,
                )
                exit(1)
            else:
                self.export_rate_limit_exceeded(response, query)


def EventsExporter(Exporter):
    def __init__(self):
        super(EventsExporter,self).__init__()
        self.sources = []

    def _get(self, st:datetime, et: datetime, query:str) -> requests.Response:
        return requests.get(
            "https://api.datadoghq.com/api/v1/query",
            headers=self.headers(self.account),
            params={
                "from": int(st.timestamp()),
                "to": int(et.timestamp()),
                "query": query,
            },
        )

@click.group()
def main():
    pass


@main.command(name="metrics-names")
@click.option(
    "--account", required=False, default="DEFAULT", help="name of the Datadog account."
)
@click.option(
    "--start-time",
    required=False,
    default=datetime(1970, 1, 1, tzinfo=pytz.UTC),
    type=click_argument_types.DateTime(),
    help="of the creation of the metric, default 1970-01-01T00:00z",
)
@click.option(
    "--pattern",
    required=False,
    default=".*",
    type=click_argument_types.RegEx(),
    help="regular expression of metrics to list, default .*"
)
def list_metrics_names(account: str, start_time: datetime, pattern: re.Pattern):
    exporter = Exporter()
    exporter.account = account
    exporter.start_time = start_time
    exporter.connect()
    exporter.get_metrics()
    for metric in filter(lambda m: pattern.fullmatch(m), exporter.metrics):
        print(metric)





def metric_window(start_time: datetime, end_time:datetime, window: Duration) -> (datetime, datetime):
    from pytz import UTC
    now = datetime.now().astimezone(UTC).replace(second=0, microsecond=0)

    if int(window.seconds / 3600):
        now = now.replace(minute=0)

    if not start_time:
        start_time = now - timedelta(seconds=window.seconds)

    if not end_time:
        end_time = now

    if end_time > now:
        end_time = now

    return (start_time, end_time)

@main.command(name="metrics")
@click.option(
    "--account", required=False, default="DEFAULT", help="name of the Datadog account."
)
@click.option(
    "--start-time",
    required=False,
    type=click_argument_types.DateTime(),
    help="of the export, default the previous hour",
)
@click.option(
    "--end-time",
    required=False,
    type=click_argument_types.DateTime(),
    help="of the export, default the start time one hour.",
)
@click.option(
    "--window",
    required=False,
    type=click_argument_types.Duration(),
    help="size of an export window",
)
@click.argument("query", required=True, nargs=-1)
def export_metrics(
    account: str, start_time: datetime, end_time: datetime, window: Duration, query
):

    start_time, end_time = metric_window(start_time, end_time, window)

    exporter = Exporter()
    exporter.account = account
    exporter.start_time = start_time
    exporter.end_time = end_time
    if window:
        exporter.window = window
    exporter.connect()
    for q in query:
        exporter.export(q)


@main.command(name="events")
@click.option(
    "--account", required=False, default="DEFAULT", help="name of the Datadog account."
)
@click.option(
    "--start-time",
    required=False,
    type=click_argument_types.DateTime(),
    help="of the export, default the previous hour",
)
@click.option(
    "--end-time",
    required=False,
    type=click_argument_types.DateTime(),
    help="of the export, default the current hour.",
)
@click.option(
    "--window",
    required=False,
    type=click_argument_types.Duration(),
    help="size of an export window, default 1h",
)
@click.argument("query", required=True, nargs=-1)
def export_metrics(
        account: str, start_time: datetime, end_time: datetime, window: Duration, query
):
    start_time, end_time = metric_window(start_time, end_time, window)
    exporter = Exporter()
    exporter.account = account
    exporter.start_time = start_time
    exporter.end_time = end_time
    if window:
        exporter.window = window
    exporter.connect()
    for q in query:
        exporter.export(q)



if __name__ == "__main__":
    main()

import json
import logging
import re
import sys
from copy import deepcopy
from datetime import datetime, timedelta

import click
from datadog_export import click_argument_types
import pytz
import requests
from typing import Optional, List
from datadog_export.config import connect, get_headers
from durations import Duration


class RateLimit(object):
    def __init__(self, headers: dict):
        self.limit = int(headers["X-RateLimit-Limit"])
        self.remaining = int(headers["X-RateLimit-Remaining"])
        self.period = int(headers["X-RateLimit-Period"])
        self.reset = int(headers["X-RateLimit-Reset"])


class Exporter(object):
    def __init__(self, start_time:Optional[datetime], end_time:datetime, window: Duration):
        super(Exporter,self).__init__()
        self.account: str = "DEFAULT"
        self.window: Duration = window
        self.end_time: datetime = end_time
        self.start_time: datetime = start_time
        self.iso_date_formats = False
        self.pretty_print = False
        self.metrics = []

    def connect(self):
        connect(self.account)

    @property
    def start_time(self) -> datetime:
        if not self._start_time:
            now = datetime.now().astimezone(pytz.UTC).replace(second=0, microsecond=0)
            if int(self.window.seconds / 3600):
                now = now.replace(minute=0)
            self._start_time = now - timedelta(seconds=self.window.seconds)

        return self._start_time

    @start_time.setter
    def start_time(self, start_time):
        self._start_time = start_time.astimezone(pytz.UTC) if start_time else None

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
        logging.info(
            'datadog-export metrics --start-time %s --end-time %s --window "%s" "%s"',
            self.start_time.isoformat(),
            self.end_time.isoformat(),
            self.window.representation,
            query,
        )

    def _get(self, st: datetime, et: datetime, query: str) -> requests.Response:
        return requests.get(
            "https://api.datadoghq.com/api/v1/query",
            headers=get_headers(self.account),
            params={
                "from": int(st.timestamp()),
                "to": int(et.timestamp()),
                "query": query,
            },
        )

    @staticmethod
    def to_datetime(ts: float) -> datetime:
        result = datetime.fromtimestamp(ts / 1000)
        result = result + timedelta(microseconds=ts % 1000)
        return result

    def convert_to_timestamps(self, response):
        if not self.iso_date_formats:
            return response

        r = deepcopy(response)
        if "from_date" in r:
            r["from_date"] = self.to_datetime(r["from_date"]).isoformat()
            r["to_date"] = self.to_datetime(r["to_date"]).isoformat()
        for s in r["series"]:
            s["start"] = self.to_datetime(s["start"]).isoformat()
            s["end"] = self.to_datetime(s["end"]).isoformat()
            for point in s["pointlist"]:
                point[0] = self.to_datetime(point[0]).isoformat()
        return r

    def process(self, response):
        if response["status"] != "error":
            r = self.convert_to_timestamps(response)
            print(json.dumps(r))
        else:
            logging.error(response["error"])
            exit(1)

    def export(self, query):
        logging.info(
            f"exporting from {self.start_time} to {self.end_time} in {self.window} steps"
        )
        st = self.start_time
        while st < self.end_time:
            et = st + timedelta(seconds=self.window.to_seconds())
            headers = get_headers(self.account)
            response = self._get(st, et, query)
            if response.status_code == 200:
                self.process(response.json())
                st = st + timedelta(seconds=self.window.to_seconds())

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


class EventsExporter(Exporter):
    def __init__(self, start_time:Optional[datetime], end_time:datetime, window:Duration, sources: List[str]):
        super(EventsExporter, self).__init__(start_time, end_time, window)
        self.sources = sources

    def convert_to_timestamps(self, response):
        if not self.iso_date_formats:
            return response

        r = deepcopy(response)
        for e in r.get("events",[]):
            e["date_happened"] = self.to_datetime(e["date_happened"]* 1000).isoformat()
            for c in e.get("children", []):
                c["date_happened"] = self.to_datetime(c["date_happened"]*1000).isoformat()
        return r


    def process(self, response):
        r = self.convert_to_timestamps(response)
        json.dump(r, sys.stdout, indent=2 if self.pretty_print else 0)

    def _get(self, st: datetime, et: datetime, query: str) -> requests.Response:
        params={
            "start": int(st.timestamp()),
            "end": int(et.timestamp()),
        }
        if query:
            params["query"] = query

        if self.sources:
            params["sources"] = ','.join(self.sources)

        if self.tags:
            params["tags"] = ','.join(self.tags)

        return requests.get(
            "https://api.datadoghq.com/api/v1/events",
            headers=get_headers(self.account),
            params=params
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
    help="regular expression of metrics to list, default .*",
)
def list_metrics_names(account: str, start_time: datetime, pattern: re.Pattern):
    exporter = Exporter()
    exporter.account = account
    exporter.start_time = start_time
    exporter.connect()
    exporter.get_metrics()
    for metric in filter(lambda m: pattern.fullmatch(m), exporter.metrics):
        print(metric)





@main.command(name="metrics")
@click.option(
    "--account", required=False, default="DEFAULT", help="name of the Datadog account."
)
@click.option(
    "--start-time",
    required=False,
    type=click_argument_types.DateTime(),
    help="of the export, default the previous 24 hours",
)
@click.option(
    "--end-time",
    required=False,
    default=datetime.now().astimezone(pytz.UTC).replace(second=0, microsecond=0),
    type=click_argument_types.DateTime(),
    help="of the export, default now",
)
@click.option(
    "--window",
    required=False,
    default=Duration("24h"),
    type=click_argument_types.Duration(),
    help="size of an export window, default 24h",
)
@click.argument("query", required=True, nargs=-1)
def export_metrics(
    account: str, start_time: datetime, end_time: datetime, window: Duration, query
):
    exporter = Exporter(start_time, end_time, window)
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
    help="of the export, default the previous time window",
)
@click.option(
    "--end-time",
    required=False,
    default=datetime.now().astimezone(pytz.UTC).replace(second=0, microsecond=0),
    type=click_argument_types.DateTime(),
    help="of the export, default now",
)
@click.option(
    "--window",
    required=False,
    default=Duration("1h"),
    type=click_argument_types.Duration(),
    help="size of an export window, default 1h",
)
@click.option(
    "--source",
    required=False,
    type=str,
    multiple=True,
    help="name of event source of the event"
)
@click.option(
    "--tag",
    required=False,
    type=str,
    multiple=True,
    help="tag on the event"
)
@click.argument("query", required=True, nargs=-1)
def export_events(
    account: str, start_time: datetime, end_time: datetime, window: Duration, query, source : List[str], tag: List[str]
):
    exporter = EventsExporter(start_time, end_time, window, source, tag)
    exporter.connect()
    for q in query:
        exporter.export(q)


if __name__ == "__main__":
    main()

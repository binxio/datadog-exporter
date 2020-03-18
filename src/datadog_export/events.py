from datadog_export.logger import log
from copy import deepcopy
from datetime import datetime
from typing import List, Optional

import click
import pytz
import requests
from re import Pattern, compile
from datadog_export import click_argument_types
from datadog_export.exporter import Exporter
from durations import Duration


class EventsExporter(Exporter):
    def __init__(
        self,
        account: str,
        start_time: Optional[datetime],
        end_time: datetime,
        window: Duration,
    ):
        super(EventsExporter, self).__init__(account, start_time, end_time, window)
        self.sources = []
        self.tags = []
        self.aggregated: bool = True
        self.priority: str = None
        self.query = None
        self.pattern: Pattern = None

    def export_started(self):
        log.info(
            f"exporting events from {self.start_time} to {self.end_time} in {self.window.representation} steps"
        )

    def convert_to_timestamps(self, response):
        if not self.iso_date_formats:
            return response

        r = deepcopy(response)
        for e in r.get("events", []):
            e["date_happened"] = self.to_datetime(e["date_happened"] * 1000).isoformat()
            for c in e.get("children", []):
                c["date_happened"] = self.to_datetime(
                    c["date_happened"] * 1000
                ).isoformat()
        return r

    def event_matched(self, event):
        return (
            self.pattern is None
            or self.pattern.findall(event.get("title", ""))
            or self.pattern.findall(event.get("text", ""))
        )

    def process(self, response):
        before = len(response["events"])
        response["events"] = list(filter(self.event_matched, response["events"]))
        after = len(response["events"])
        if self.pattern:
            log.info(f"{after} out of {before} events matched")
        r = self.convert_to_timestamps(response)
        self.write(r)

    def _get(self, st: datetime, et: datetime) -> requests.Response:
        params = {
            "start": int(st.timestamp()),
            "end": int(et.timestamp()),
            "aggregated": str(self.aggregated).lower(),
        }

        if self.sources:
            params["sources"] = ",".join(self.sources)

        if self.tags:
            params["tags"] = ",".join(self.tags)

        if self.priority:
            params["priority"] = self.priority

        return requests.get(
            "https://api.datadoghq.com/api/v1/events",
            headers=self.get_headers(),
            params=params,
        )


@click.command(name="events")
@click.option(
    "--account", required=False, default="DEFAULT", help="name of the Datadog account."
)
@click.option(
    "--start-time",
    required=True,
    type=click_argument_types.DateTime(),
    help="of the export. either a duration, date or timestamp",
)
@click.option(
    "--end-time",
    required=False,
    default=datetime.now().astimezone(pytz.UTC).replace(second=0, microsecond=0),
    type=click_argument_types.DateTime(),
    help="of the export. either a duration, date or timestamp. default now",
)
@click.option(
    "--window",
    required=False,
    default=Duration("24h"),
    type=click_argument_types.Duration(),
    help="size of an export window, default 24h",
)
@click.option(
    "--iso-datetime/--no-iso-datetime",
    required=False,
    default=False,
    help="output timestamps in iso format",
)
@click.option(
    "--pretty-print/--no-pretty-print",
    required=False,
    default=False,
    help="output json in pretty print",
)
@click.option(
    "--source", required=False, type=str, multiple=True, help="to filter events on"
)
@click.option(
    "--tag", required=False, type=str, multiple=True, help="to filter events on"
)
@click.option(
    "--priority",
    required=False,
    type=click.Choice(["low", "normal"]),
    help="to filter events on",
)
@click.option(
    "--aggregated/--unaggregated",
    required=False,
    default=True,
    help="add events initiated before start time",
)
@click.option(
    "--pattern",
    type=click_argument_types.RegEx(),
    help="to search for in filter, default .*",
)
def main(
    account: str,
    start_time: datetime,
    end_time: datetime,
    window: Duration,
    iso_datetime: bool,
    pretty_print: bool,
    source: Optional[List[str]],
    tag: Optional[List[str]],
    priority: Optional[str],
    aggregated: bool,
    pattern: Optional[Pattern],
):
    """
    export datadog events.
    """
    exporter = EventsExporter(account, start_time, end_time, window)
    exporter.iso_date_formats = iso_datetime
    exporter.pretty_print = pretty_print
    exporter.sources = source
    exporter.tags = tag
    exporter.priority = priority
    exporter.aggregated = aggregated
    exporter.pattern = pattern
    exporter.connect()
    exporter.export()


if __name__ == "__main__":
    main()

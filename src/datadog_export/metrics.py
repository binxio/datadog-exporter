from datadog_export.logger import log
from copy import deepcopy
from datetime import datetime
from typing import Optional

import click
import pytz
import requests
from durations import Duration

from datadog_export import click_argument_types
from datadog_export.exporter import Exporter


class MetricsExporter(Exporter):
    def __init__(
        self,
        account: str,
        start_time: Optional[datetime],
        end_time: datetime,
        window: Duration,
    ):
        super(MetricsExporter, self).__init__(account, start_time, end_time, window)
        self.query = None

    def export_started(self):
        log.info(
            f"exporting {self.query} from {self.start_time} to {self.end_time} in {self.window.representation} steps"
        )

    def _get(self, st: datetime, et: datetime) -> requests.Response:
        return requests.get(
            "https://api.datadoghq.com/api/v1/query",
            headers=self.get_headers(),
            params={
                "from": int(st.timestamp()),
                "to": int(et.timestamp()),
                "query": self.query,
            },
        )

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
            self.write(r)
        else:
            log.error(response["error"])
            exit(1)


@click.command(name="metrics")
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
    help="of the export. either a duration, date or timestamp. default now.",
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
@click.argument("query", required=True, nargs=-1)
def main(
    account: str,
    start_time: datetime,
    end_time: datetime,
    window: Duration,
    iso_datetime: bool,
    pretty_print: bool,
    query,
):
    """
    export datadog metrics.
    """
    exporter = MetricsExporter(account, start_time, end_time, window)
    exporter.iso_date_formats = iso_datetime
    exporter.pretty_print = pretty_print
    exporter.connect()
    for q in query:
        exporter.query = q
        exporter.export()


if __name__ == "__main__":
    main()

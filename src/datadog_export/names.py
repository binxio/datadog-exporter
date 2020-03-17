from datetime import datetime
import requests
from durations import Duration
from typing import Optional, List
import pytz
from re import Pattern
from datadog_export.exporter import Exporter
import click

from datadog_export import click_argument_types


class MetricNamesExporter(Exporter):
    def __init__(self, account: str, start_time: Optional[datetime]):
        end_time = datetime.now().astimezone(pytz.UTC)
        window = Duration("{}s".format((end_time - start_time).total_seconds() + 1))
        super(MetricNamesExporter, self).__init__(account, start_time, end_time, window)
        self.hosts = []
        self.metrics = []

    def _get(self, st: datetime, et: datetime) -> requests.Response:
        params = {"from": int(st.timestamp())}
        if self.hosts:
            params["hosts"] = self.hosts

        return requests.get(
            "https://api.datadoghq.com/api/v1/metrics",
            headers=self.get_headers(),
            params=params,
        )

    def process(self, response: dict):
        self.metrics.extend(response["metrics"])


@click.command(name="names")
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
@click.option("--host", required=False, multiple=True, help="to obtain metrics from")
def main(account: str, start_time: datetime, pattern: Pattern, host: List[str]):
    """
    export datadog metric names.
    """

    exporter = MetricNamesExporter(account, start_time)
    exporter.hosts = host
    exporter.connect()
    exporter.export()
    for metric in filter(lambda m: pattern.fullmatch(m), exporter.metrics):
        print(metric)


if __name__ == "__main__":
    main()

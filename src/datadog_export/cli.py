from datadog_export.logger import log

import click
import os

from datadog_export.events import main as export_events
from datadog_export.metrics import main as export_metrics
from datadog_export.names import main as export_names


@click.group()
def main():
    pass


main.add_command(export_names)
main.add_command(export_metrics)
main.add_command(export_events)


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"), format="%(levelname)s: %(message)s"
    )
    main()

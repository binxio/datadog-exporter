from datetime import datetime, timedelta

import click
from dateutil.parser import parse
from pytz import UTC
from tzlocal import get_localzone
import durations


class DateTime(click.ParamType):
    """
    A datetime object parsed via dateutil.parser.parse. the resulting
    datetime is timezone aware and set to UTC. If no timezone is
    specified in the string, the local timezone is assumed.
    """

    name = "datetime"

    def __init__(self):
        self.tz = get_localzone()

    def to_utc(self, dt: datetime):
        result = dt
        if not result.tzinfo:
            result = self.tz.localize(result)
        return result.astimezone(UTC)

    def convert(self, value, param, ctx) -> datetime:
        if value is None:
            return value

        if isinstance(value, datetime):
            return self.to_utc(value)

        try:
            if isinstance(value, str) and value[0] in ["+", "-"]:
                duration = durations.Duration(value)
                now = datetime.now()
                st = now.replace(second=0, microsecond=0)
                st = self.to_utc(st + timedelta(seconds=duration.seconds))
            else:
                st = self.to_utc(parse(value))
            return st
        except ValueError as e:
            self.fail(f'Could not parse "{value}" into datetime ({e})', param, ctx)


1

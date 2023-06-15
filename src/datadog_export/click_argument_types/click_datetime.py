from datetime import datetime, timedelta

import click
from dateutil.parser import parse
import pytz
from tzlocal import get_localzone
import durations
from typing import Optional


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
            result = result.astimezone(result.tzinfo)
        return result.astimezone(pytz.UTC)

    def convert(self, value, param, ctx) -> Optional[datetime]:
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

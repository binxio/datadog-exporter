import json
from datadog_export.logger import log
from datetime import datetime, timedelta
from typing import Optional

import pytz
import requests
import sys
from datadog_export.config import connect, get_headers
from durations import Duration


class RateLimit(object):
    def __init__(self, headers: dict):
        self.limit = int(headers.get("X-RateLimit-Limit", "0"))
        self.remaining = int(headers.get("X-RateLimit-Remaining", "0"))
        self.period = int(headers.get("X-RateLimit-Period", "0"))
        self.reset = int(headers.get("X-RateLimit-Reset", "0"))

    def __str__(self):
        if self.limit:
            return "rate limit of {limit} API calls per {period}s. {remaining} remaining, reset in {reset}s".format(
                **self.__dict__
            )
        else:
            return ""


class Exporter(object):
    def __init__(
        self,
        account: str,
        start_time: Optional[datetime],
        end_time: datetime,
        window: Duration,
    ):
        super(Exporter, self).__init__()
        self.account: str = account if account else "DEFAULT"
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

    def export_rate_limit_exceeded(self, response):
        rate_limit = RateLimit(response.headers)
        log.error(
            "rate limit exceeded of %s calls to %s in %ss, retry in %ss",
            rate_limit.limit,
            response.url,
            rate_limit.period,
            rate_limit.reset,
        )

    def get_headers(self):
        return get_headers(self.account)

    def _get(self, st: datetime, et: datetime) -> requests.Response:
        raise Exception("not implemented")

    @staticmethod
    def to_datetime(ts: float) -> datetime:
        result = datetime.fromtimestamp(ts / 1000)
        result = result + timedelta(microseconds=ts % 1000)
        return result

    def write(self, response):
        kwargs = {}
        if self.pretty_print:
            kwargs["indent"] = 2
        json.dump(response, sys.stdout, **kwargs)

    def process(self, response: dict):
        self.write(response)

    def export_started(self):
        log.info(
            f"exporting from {self.start_time} to {self.end_time} in {self.window} steps"
        )

    def export_completed(self):
        log.info(f"export complete. {self.ratelimit}")

    def export(self):
        self.export_started()
        st = self.start_time
        while st < self.end_time:
            et = st + timedelta(seconds=self.window.to_seconds())
            response = self._get(st, et)
            self.ratelimit = RateLimit(response.headers)
            if response.status_code == 200:
                self.process(response.json())
                st = st + timedelta(seconds=self.window.to_seconds())

            elif response.status_code != 429:
                log.error(
                    "%s for %s returned %s, %s",
                    response.request.url,
                    response.status_code,
                    response.text,
                )
                exit(1)
            else:
                self.export_rate_limit_exceeded(response)
        self.export_completed()

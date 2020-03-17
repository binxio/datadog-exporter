from datetime import datetime
import requests
from durations import Duration
from typing import Optional,List
from copy import deepcopy
import sys, json
import logging
from datadog_export.exporter import  Exporter

class MetricsExporter(Exporter):
    def __init__(self, start_time:Optional[datetime], end_time:datetime, window: Duration):
        super(MetricsExporter,self).__init__(start_time, end_time, window)

    def _get(self, st: datetime, et: datetime, query: str) -> requests.Response:
        return requests.get(
            "https://api.datadoghq.com/api/v1/query",
            headers=self.get_headers(),
            params={
                "from": int(st.timestamp()),
                "to": int(et.timestamp()),
                "query": query,
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
            json.dump(r, sys.stdout)
        else:
            logging.error(response["error"])
            exit(1)

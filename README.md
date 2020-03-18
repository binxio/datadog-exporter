A simple command line utility to export datadog metrics.

## events
to export datadog events that where reported in the last 24 hours, type:

```
datadog-exporter events \
    --iso-datetime \
    --pretty-print \
    --start-time -24h
```

## metric names
to export all available database metric names, type:

```
datadog-exporter names
```

## metrics
to export specific metrics from the last 24 hours, type:

```
 datadog_export metrics \
    --iso-datetime \
    --pretty-print \
    --start-time -2h \
    --window 30m \
    'docker.cpu.system{*}'     
```
The `--window` option allows you to influence the resolution of the values returned. 

# credentials
The file ~/.datadog.ini is a Python configuration file from which the Datadog i
connection parameters are read.  At least it will
need the `api_key` and `api_app` attributes in the section DEFAULT::

	[DEFAULT]
	api_key=a77aaaaaaaaaaaaaaaaaaaaa
	app_key=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb

Alternatively, you can set the environment variable `DATADOG_API_KEY`  and `DATADOG_APP_KEY`.

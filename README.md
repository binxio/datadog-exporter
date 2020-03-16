A simple command line utility to export datadog metrics.


# credentials
The file ~/.datadog.ini is a Python configuration file from which the Datadog connection parameters are read.  At least it will
need the `api_key` and `api_app` attributes in the section DEFAULT::

	[DEFAULT]
	api_key=a77aaaaaaaaaaaaaaaaaaaaa
	app_key=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb


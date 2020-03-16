import re

import click


class RegEx(click.ParamType):
    """
    A regular expression
    """

    name = "regex"

    def __init__(self):
        pass


    def convert(self, value, param, ctx) -> re.Pattern:
        if value is None:
            return value

        if isinstance(value, re.Pattern):
            return value

        try:
            return re.compile(value)
        except ValueError as e:
            self.fail(f'Could not parse "{value}" into regex pattern ({e})', param, ctx)

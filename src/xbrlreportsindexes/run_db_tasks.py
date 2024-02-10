"""Entry point for command line tasks"""
from __future__ import annotations

import argparse
import gettext
import sys

from xbrlreportsindexes.cmd_line_util import args_dict
from xbrlreportsindexes.cmd_line_util import args_search_group
from xbrlreportsindexes.cmd_line_util import run_args
from xbrlreportsindexes.cmd_line_util import run_refresher

args_parser = argparse.ArgumentParser(
    description=(
        "Run initialize, update and search tasks on database. "
        "Connection parameters are required argument in the form: "
        "database_name,product,user,password,host,port,timeout "
        "(no spaces), product can be one of [postgres sqlite "
        "mysql], database_name and product are required."
    )
)

for k, v in args_dict.items():
    args_parser.add_argument(k, **v)

search_group = args_parser.add_argument_group(
    "DB search options",
    "Parameters to search and retrieve filings from db_instance",
)

for k, v in args_search_group.items():
    search_group.add_argument(k, **v)


refresher_args = argparse.ArgumentParser(
    description=(
        "Refreshes S&P, NASDAQ, DJIA companies tags. "
    )
)

refresher_args.add_argument("connection", **args_dict["connection"])
refresher_args.add_argument(
    "--initialize-database", **args_dict["--initialize-database"]
)

def main() -> int:
    """Entry point for CLI"""
    gettext.install("arelle")
    options = args_parser.parse_args()
    run_args(options)
    return 0

def refresher() -> int:
    """Entry point for CLI"""
    gettext.install("arelle")
    options = refresher_args.parse_args()
    run_refresher(options)
    return 0


if __name__ == "__main__":
    sys.exit(main())

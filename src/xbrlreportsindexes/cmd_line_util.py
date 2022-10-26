"""Utilities for command line task execution"""
from __future__ import annotations

import argparse
import datetime
import json
import signal
import sys
from typing import Any
from typing import Optional

from sqlalchemy.orm import Session
from xbrlreportsindexes.core import constants
from xbrlreportsindexes.core import index_db
from xbrlreportsindexes.core.constants import ERR_BAD_CMD_PARAM
from xbrlreportsindexes.core.constants import XIDBException


def handle_main_termination(*args: Any) -> None:
    """Always raise SystemExit to trigger cleanup."""
    raise SystemExit


signal.signal(signal.SIGTERM, handle_main_termination)
signal.signal(signal.SIGINT, handle_main_termination)
if sys.platform != "win32":
    signal.signal(signal.SIGQUIT, handle_main_termination)

args_dict: dict[str, dict[str, Any]] = {
    "connection": {
        "help": (
            "Connection parameters is required in the form "
            "database_name,product,user,password,host,port,timeout (no spaces)"
            "product can be one of [postgres sqlite mysql] (defaults to sqlite), "
            "database_name is required."
        )
    },
    "--initialize-database": {
        "action": "store_true",
        "dest": "initialize_db",
        "help": "Creates missing tables and load cached data.",
    },
    "--update-esef": {
        "action": "store_true",
        "dest": "update_esef_index",
        "help": "Update ESEF filing index to current date.",
    },
    "--update-sec": {
        "action": "store_true",
        "dest": "update_sec",
        "help": "Updates all available SEC xbrl filings list "
        "up to current date, including latest filings.",
    },
    "--update-sec-date-from": {
        "action": "store",
        "dest": "update_sec_date_from",
        "help": "Limits SEC xbrl filings list update to feeds FROM "
        "the specified date, date should be in the format "
        "2022-02-22.",
    },
    "--update-sec-date-to": {
        "action": "store",
        "dest": "update_sec_date_to",
        "help": "Limits SEC xbrl filings list update to feeds TO the "
        "specified date, date should be in the format "
        "2022-02-22.",
    },
    "--update-sec-include-latest": {
        "action": "store_true",
        "default": True,
        "dest": "update_sec_include_latest",
        "help": "Whether to get latest SEC XBRL filings included in "
        "https://www.sec.gov/Archives/edgar/xbrlrss.all.xml, "
        "this also causes getting latest month filings even "
        "if not selected in date range. [Default]",
    },
    "--update-sec-filers": {
        "action": "store",
        "dest": "update_sec_filers_choice",
        "choices": ["quick", "by-date", "list"],
        "help": "Updates sec filers information stored in the database "
        "using one of 3 options:\nquick [default] -> Updates "
        "filers appearing to have discrepancies between existing "
        "information and information from last filing. by-date -> "
        "Updates filers created in database before date specified "
        "by --update-sec-filer-param. "
        "list -> Updates files from a given list of cik numbers "
        "specified by --update-sec-filer-param in the format "
        "0000000000,0000000000,...",
    },
    "--update-sec-filer-param": {
        "action": "store",
        "dest": "update_sec_filers_param",
        "help": "If --update-sec-filers is is `by-date` date must be "
        "given in this option in format 2022-02-22\n If "
        "--update-sec-filers is is `list` ciks to update "
        "must be given in this option in the format "
        "000000000,0000000000...",
    },
    "--list-countries": {
        "action": "store_true",
        "dest": "list_countries",
        "help": "Print out list of countries and information available "
        "code, name, alpha-2 name, long., lat.",
    },
    "--list-industry-tree": {
        "action": "store",
        "dest": "list_industry_tree",
        "help": "Print out industry tree (sub classifications) for the given"
        " industry codes receives json like string as follows:"
        '\'{"codes":[1,1000, ...], "classification": "SEC|SIC|NAICS", '
        '"style": "csv|indented"}\'',
    },
}

args_search_group: dict[str, dict[str, Any]] = {
    "--search-filings": {
        "action": "store",
        "choices": ["esef", "sec"],
        "dest": "search_filing_system_choice",
        "help": "Flag to search filings, `sec` or `esef`, requires at least "
        "one search parameter from --added-date-from, --added-date-to "
        ", --report-date-from, --report-date-to, --form-type, "
        "--filing-number, --filer-identifier, --filer-name, "
        "--ticker-symbol, --industry-code. ticker-symbol, "
        "industry-code, form-type are not valid search fields "
        "when `esef` is selected",
    },
    "--added-date-from": {
        "action": "store",
        "dest": "add_date_from",
        "help": "Earliest date to search for when was the report published "
        "or added to index, formatted 2022-02-22.",
    },
    "--added-date-to": {
        "action": "store",
        "dest": "add_date_to",
        "help": "Latest date to search for when was the report published "
        "or added to index, formatted 2022-02-22.",
    },
    "--report-date-from": {
        "action": "store",
        "dest": "report_date_from",
        "help": "Earliest report date to search, formatted 2022-02-22.",
    },
    "--report-date-to": {
        "action": "store",
        "dest": "report_date_to",
        "help": "Latest report date to search, formatted 2022-02-22.",
    },
    "--form-type": {
        "action": "store",
        "dest": "form_type",
        "help": "From to lookup, 10-k, 10-q..., not valid for esef.",
    },
    "--filing-number": {
        "action": "store",
        "dest": "filing_number",
        "help": "Filing number, accession number in case of SEC or "
        "filing key in case of ESEF, multiples can be comma "
        "separated.",
    },
    "--filer-identifier": {
        "action": "store",
        "dest": "filer_identifier",
        "help": "CIK number in case of SEC or LEI in case of ESEF, "
        "multiples can be comma separated.",
    },
    "--filer-name": {
        "action": "store",
        "dest": "filer_name",
        "help": "Full or partial company name(s), multiples can be "
        "comma separated.",
    },
    "--sec-ticker-symbol": {
        "action": "store",
        "dest": "ticker_symbol",
        "help": "Full or partial ticker symbols(s), multiples can be "
        "comma separated, IGNORED in ESEF searches.",
    },
    "--sec-industry-code": {
        "action": "store",
        "dest": "industry_code",
        "help": "SEC industry classification code as presented on "
        "https://www.sec.gov/corpfin/division-of-corporation-"
        "finance-standard-industrial"
        "-classification-sic-code-list, multiples can be comma "
        "separated, IGNORED in ESEF searches or when "
        "`--sec-industry-tree`"
        "has a value.",
    },
    "--sec-industry-tree": {
        "action": "store",
        "dest": "industry_tree",
        "help": "SEC industry classification PARENT code from classifications"
        " presented on "
        "https://www.sec.gov/corpfin/division-of-corporation-"
        "finance-standard-industrial-classification-sic-code-list,"
        " this option returns ALL sub-classifications under the given"
        " code, multiples can be comma, separated, IGNORED in ESEF "
        "searches. use --xri-db-task  <db connection params> "
        '--list-industry-tree \'{"codes":[1,1000, ...], '
        '"classification": "SEC"}\' to list industry tree for '
        "given code, omit codes to list all SEC industries.",
    },
    "--sec-industry-name": {
        "action": "store",
        "dest": "industry_name",
        "help": "SEC industry classification full or partial names "
        "as presented on "
        "https://www.sec.gov/corpfin/division-of-corporation-"
        "finance-standard-industrial-classification-sic-code-list"
        ", multiples can be comma separated, IGNORED in ESEF "
        "searches or if any of --sec-industry-code, or "
        "--sec-industry-tree has value.",
    },
    "--esef-country-alpha2": {
        "action": "store",
        "dest": "esef_country_alpha2",
        "help": "Valid alpha-2 country code or comma separated codes, ignored"
        "in SEC searches. try xri-db-tasks <db params> "
        "--list-countries",
    },
    "--esef-country-name": {
        "action": "store",
        "dest": "esef_country_name",
        "help": "Full or partial country name, or comma separated names,"
        " , ignored in SEC searches or if --esef-country-alpha2 "
        "has value. try xri-db-tasks <db params> --list-countries",
    },
    "--limit-result": {
        "action": "store",
        "default": 20,
        "type": int,
        "dest": "limit_result",
        "help": "Limit number of filings returned from the query Default 20",
    },
    "--random-result": {
        "action": "store_true",
        "dest": "random_result",
        "help": "Returns a random subset of the query based on limit, "
        "if limit is not given, forced limit of 20.",
    },
    "--output-file": {
        "action": "store",
        "dest": "output_file",
        "help": "File to save query result, defaults to "
        "`filing_search_YYMMDDHHmmss.rss` in current directory, "
        "file is an rss feed that can be loaded by arelle, if file "
        "is specified with xxx.json, output will be in json format.",
    },
}


def make_db_connection(options: argparse.Namespace) -> index_db.XbrlIndexDB:
    """Make connection using positional args"""

    db_conn = [x.strip() for x in options.connection.split(",")]

    if len(db_conn) < 1:
        raise constants.XIDBException(
            constants.ERR_BAD_CONN_PARAM,
            "At least database name is required for a connection.",
        )

    # database_name,product,user,password,host,port,timeout
    conn_params = {}
    conn_params["database"] = db_conn[0]
    conn_params["product"] = "sqlite"
    if len(db_conn) > 1 and len(db_conn) <= 7:
        conn_params["product"] = db_conn[1]
        conn_params["user"] = db_conn[2]
        conn_params["password"] = db_conn[3]
        conn_params["host"] = db_conn[4]
        conn_params["port"] = db_conn[5]
    if len(db_conn) == 7:
        conn_params["timeout"] = db_conn[6]

    db_instance = index_db.XbrlIndexDB(**conn_params)

    if options.initialize_db:
        db_instance.verify_initialize_database(options.initialize_db)
    return db_instance


def do_update_tasks(
    options: argparse.Namespace, db_instance: index_db.XbrlIndexDB
) -> None:
    """Perform update tasks based on options"""
    try:
        if options.update_esef_index:
            db_instance.update_esef_default()

        if options.update_sec:
            db_instance.update_sec_default(
                from_date=options.update_sec_date_from,
                to_date=options.update_sec_date_to,
                include_latest=options.update_sec_include_latest,
                reload_cache=True,
            )
        if options.update_sec_filers_choice:
            if options.update_sec_filers_choice == "quick":
                db_instance.filers_quick_update()
            elif options.update_sec_filers_choice == "list":
                if options.update_sec_filers_param is None:
                    raise constants.XIDBException(
                        constants.ERR_BAD_CMD_PARAM,
                        "--update-sec-filer-param should contain a "
                        "list of filers ciks.",
                    )
                filers_to_update = [
                    x.strip() for x in options.update_sec_filers_param
                ]
                db_instance.filers_update_ciks(filers_to_update)
            elif options.update_sec_filers_choice == "by-date":
                db_instance.filers_update_by_date(
                    options.update_sec_filers_param
                )
    except (SystemExit, Exception) as err:
        print("Error handled")
        note = str(err)[:50] if len(str(err)) > 50 else str(err)
        db_instance.do_exit_cleanup(False, True, note)
        raise


def do_search_tasks(
    options: argparse.Namespace, db_instance: index_db.XbrlIndexDB
) -> Optional[str]:
    """Perform search tasks base on options"""
    file = None
    if options.search_filing_system_choice:
        query_params = [
            options.add_date_to,
            options.add_date_from,
            options.report_date_from,
            options.report_date_to,
            options.filing_number,
            options.filer_identifier,
            options.filer_name,
            options.form_type,
            options.ticker_symbol,
            options.industry_code,
            options.output_file,
            options.industry_tree,
            options.industry_name,
            options.esef_country_alpha2,
            options.esef_country_name,
        ]
        if not any(query_params):
            raise constants.XIDBException(
                constants.ERR_BAD_CMD_PARAM,
                "At lease one search parameter must be entered from :"
                "--added-date-from, --added-date-to, --report-date-from,"
                "--report-date-to, --form-type, --filing-number, "
                "--filer-identifier, --filer-name, --ticker-symbol, "
                "--industry-code, --esef-country-name, --esef-country-alpha2,"
                "--sec-industry-name, --sec-industry-tree",
            )
        date_and_time = datetime.datetime.now()
        _file_id = date_and_time.strftime("%Y%m%d%H%M%S") + ".rss"
        out_file = options.output_file
        if not out_file:
            out_file = "query_out_" + _file_id
        out_type = "json" if out_file.lower().endswith(".json") else "rss"
        search_query = db_instance.search_filings(
            filing_system=options.search_filing_system_choice,
            publication_date_from=options.add_date_from,
            publication_date_to=options.add_date_to,
            report_date_from=options.report_date_from,
            report_date_to=options.report_date_to,
            filer_identifier=options.filer_identifier,
            filer_name=options.filer_name,
            filing_number=options.filing_number,
            filer_ticker_symbol=options.ticker_symbol,
            form_type=options.form_type,
            industry_code=options.industry_code,
            limit=options.limit_result,
            esef_country_alpha2=options.esef_country_alpha2,
            esef_country_name=options.esef_country_name,
            industry_name=options.industry_name,
            industry_code_tree=options.industry_tree,
            random=options.random_result,
        )
        if search_query:
            with Session(db_instance.engine) as session:
                search_result = search_query.with_session(session)
                file = db_instance.save_filings(
                    filings_list=search_result,
                    filename=out_file,
                    type_=out_type,
                    description=(
                        f"query at {str(date_and_time)} "
                        "database: {db_instance.database}."
                    ),
                    title="Query result",
                )
    assert isinstance(file, tuple)
    return file[0]


def list_industry_tree(
    options: argparse.Namespace, db_instance: index_db.XbrlIndexDB
) -> None:
    """Print out industry tree"""
    if options.list_industry_tree:
        try:
            params: dict[str, list[int] | str | None] = json.loads(
                options.list_industry_tree
            )
            db_instance.get_an_industry_tree(
                industry_codes=params.get(  # type: ignore[arg-type]
                    "codes", None
                ),
                industry_classification=params.get(  # type: ignore[arg-type]
                    "classification", "SEC"
                ),
                verbose=True,
                style=params.get("style", "indented"),  # type: ignore[arg-type]
            )
        except Exception as err:
            raise XIDBException(
                ERR_BAD_CMD_PARAM,
                f"{options.list_industry_tree} could not be parsed as json",
            ) from err


def list_countries_info(
    options: argparse.Namespace, db_instance: index_db.XbrlIndexDB
) -> None:
    """List available counties information"""
    if options.list_countries:
        db_instance.list_countries_info(True)


def run_args(options: argparse.Namespace) -> None:
    """Main entry point"""
    db_instance = make_db_connection(options)
    if db_instance.db_exists:
        do_update_tasks(options, db_instance)
        do_search_tasks(options, db_instance)
        list_industry_tree(options, db_instance)
        list_countries_info(options, db_instance)

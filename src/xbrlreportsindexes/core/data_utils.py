"""Utilities to help in working with the data"""
from __future__ import annotations

import datetime
import json
import logging
import os
import pathlib
import pickle
import re
import sys
import time
import traceback
from calendar import monthrange
from collections import defaultdict
from collections.abc import Iterator
from re import Pattern
from typing import Any
from typing import cast
from urllib.parse import urljoin

import pytz
from dateutil import parser
from lxml import etree
from lxml import html
from pycountry import languages  # type: ignore[import]
from sqlalchemy import Table
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from xbrlreportsindexes.core import constants
from xbrlreportsindexes.core.arelle_utils import CntlrPy
from xbrlreportsindexes.core.constants import log_template
from xbrlreportsindexes.core.constants import XIDBException
from xbrlreportsindexes.model import ESEF
from xbrlreportsindexes.model import SEC
from xbrlreportsindexes.model.base_model import replace_caps


try:
    from arelle.UrlUtil import parseRfcDatetime
    from arelle.ModelDocument import ModelDocument
    from arelle.ModelRssItem import ModelRssItem
    from arelle.Cntlr import Cntlr
    from arelle.ModelXbrl import ModelXbrl
    from arelle import XbrlConst, XmlUtil
except ModuleNotFoundError as exc:
    raise Exception("Please add path to arelle to python path") from exc


file_id_re: Pattern[str] = re.compile(r"\d{4}-\d{2}")


def truncate_string(str_x: str, _n: int = 50) -> str:
    """Truncate string to first n letters"""
    return str_x[:_n] if len(str_x) > _n else str_x


def ts_now(
    time_zone: str | None = None, round_: bool = False
) -> datetime.datetime:
    """Time stamp now, with or without timezone"""
    now = datetime.datetime.now()
    if time_zone:
        now = datetime.datetime.now(tz=pytz.timezone(time_zone))
    if round_:
        now = now.replace(microsecond=0)
    return now


def instance_to_dict(instance: DeclarativeMeta) -> dict[str, Any]:
    """Converts mapper instance to dict"""
    result_dict = {}
    table_ = getattr(instance, "__table__")
    assert isinstance(table_, Table)
    for _x in table_.columns.keys():
        result_dict[_x] = getattr(instance, _x, None)
    return result_dict


def chunks(lst: list[Any], _n: int) -> Iterator[list[Any]]:
    """Yield successive n-sized chunks from list, from SO
    https://stackoverflow.com/questions/312443/how-do-i-split-a-list-into-equally-sized-chunks
    """
    for i in range(0, len(lst), _n):
        yield lst[i : i + _n]


def get_time_elapsed(start_time: float) -> float:
    """Subtract and round time"""
    return round(time.perf_counter() - start_time, 3)


def get_monthly_rss_feeds_links(
    cntlr: Cntlr | None = None, dbname: str | None = None, loc: Any = None
) -> list[dict[str, datetime.datetime | str | bool]]:
    """Returns Monthly feeds Links with last modified to compare to those
    in DB"""
    if loc is None:
        loc = constants.MONTHLY_FEEDS_URL
    if cntlr is None:
        cntlr = CntlrPy()
    if dbname is None:
        dbname = ""
    loc_is_file: bool = False
    if isinstance(loc, str) and loc.startswith("file://"):
        loc_is_file = True
    rss_pattern: Pattern[str] = re.compile(r"^xbrlrss-\d{4}-\d{2}.xml*")
    if loc_is_file:
        rss_pattern = re.compile(r"^xbrlrss-\d{4}-\d{2}(_\d{1})?.xml*")
    year_mon_pattern: Pattern[str] = re.compile(r"\d{4}-\d{2}")
    feeds_page = None
    feeds: list[dict[str, datetime.datetime | str | bool]] = []
    try:
        is_file = loc.startswith("file://") if isinstance(loc, str) else False
        feeds_page = cntlr.webCache.opener.open(loc)
        if (feeds_page.code == 200 and not is_file) or (
            is_file and not feeds_page.closed
        ):
            cntlr.showStatus(f"Getting feeds info from {loc}")
            tree = (
                html.parse(feeds_page)
                .getroot()
                .xpath(".//table//tr[child::td]")
            )
            # get all links
            href = [
                (
                    x.xpath("td/a/@href")[0],
                    parser.parse(x.xpath("td[3]/text()")[0]),
                )
                for x in tree
            ]
            for _h in href:
                if rss_pattern.match(_h[0]):
                    _feed: dict[str, Any] = {}
                    # get year-month to use later as feed_id
                    feed_month_search = year_mon_pattern.search(_h[0])
                    assert feed_month_search is not None
                    feed_month = feed_month_search.group()
                    # convert year-month to arbitrary date
                    feed_month_date = parser.parse(feed_month)
                    # make id
                    _feed[SEC.SecFeed.feed_id.key] = int(
                        feed_month.replace("-", "")
                    )
                    # make date at the end of the month or current
                    # date whichever closer
                    feed_month_date_mod = feed_month_date.replace(
                        day=monthrange(
                            year=feed_month_date.year,
                            month=feed_month_date.month,
                        )[1]
                    )
                    if feed_month_date_mod > datetime.datetime.now():
                        feed_month_date_mod = feed_month_date_mod.replace(
                            day=datetime.datetime.now().day
                        )
                    _feed["feed_date"] = feed_month_date_mod
                    _feed["link"] = urljoin(loc, _h[0])
                    assert isinstance(SEC.SecFeed.last_modified_date.key, str)
                    _feed[SEC.SecFeed.last_modified_date.key] = _h[1]
                    _feed["is_last_month"] = False
                    feeds.append(_feed)
        else:
            cntlr.addToLog(
                (f"{loc} returned code {feeds_page.code}"),
                **log_template("error", cntlr, logging.ERROR),
            )
    except Exception as _e:
        cntlr.addToLog(
            (
                f"Error while getting feeds:\n"
                f"{str(_e)}\n{traceback.format_tb(sys.exc_info()[2])}"
            ),
            **log_template("error", cntlr, logging.ERROR),
        )

    if len(feeds) > 0:
        feeds = sorted(feeds, key=lambda x: x["feed_date"])
        feeds[-1]["is_last_month"] = True
    return feeds


def date_filter_feeds(
    all_feeds: list[dict[str, datetime.datetime | str | bool]],
    from_date: str | datetime.datetime | None = None,
    to_date: str | datetime.datetime | None = None,
    keep_last: bool = True,
) -> list[dict[str, datetime.datetime | str | bool]]:
    """Filters feeds for the selected date range, may last feed if
    not in date range to use if latest is retrieved
    """
    if from_date is None and to_date is None:
        return all_feeds
    filtered_feeds: list[dict[str, datetime.datetime | str | bool]] = []
    if isinstance(from_date, str):
        from_date = parser.parse(from_date)
    elif from_date is None:
        from_date = datetime.datetime.min

    if to_date and isinstance(to_date, str):
        _to_date = parser.parse(to_date)
    elif to_date is None:
        _to_date = datetime.datetime.max
    else:
        assert isinstance(to_date, datetime.datetime)
        _to_date = to_date
    # always get end of the month to include both start and end
    to_date = (
        _to_date.replace(
            day=monthrange(year=_to_date.year, month=_to_date.month)[1]
        )
        if to_date
        else datetime.datetime.max
    )
    if from_date > to_date:
        raise XIDBException(constants.ERR_BAD_DATE_RANGE)
    if all_feeds:
        filtered_feeds = []
        last_appended = False
        for _f in all_feeds:
            # make sure last feed is processed before the latest
            # 200 are processed if get_latest is selected in the
            # main function
            assert isinstance(_f["feed_date"], datetime.datetime)
            if _f["feed_date"] >= from_date and _f["feed_date"] <= to_date:
                filtered_feeds.append(_f)
                if _f["is_last_month"]:
                    last_appended = True
            if all([keep_last, not last_appended, _f["is_last_month"]]):
                filtered_feeds.append(_f)
    return filtered_feeds


def get_new_and_modified_feeds(
    existing_feeds_dict: dict[str, datetime.datetime],
    feeds: list[dict[str, Any]],
) -> list[dict[str, int | str | None]]:
    """Filter feeds within date selection range and sort out which is new and
    which is modified and ignores existing unmodified feeds
    """
    result: list[dict[str, int | str | None]] = []
    key = "new_or_modified"
    new_count = 0
    modified_count = 0

    assert isinstance(SEC.SecFeed.last_modified_date.key, str)

    for _x in feeds:
        if _x[SEC.SecFeed.feed_id.key] not in existing_feeds_dict:
            _x[key] = "new"
            result.append(_x)
            new_count += 1
        elif (
            _x[SEC.SecFeed.feed_id.key] in existing_feeds_dict
            and _x[SEC.SecFeed.last_modified_date.key]
            > existing_feeds_dict[_x[SEC.SecFeed.feed_id.key]]
        ):
            _x[key] = "modified"
            result.append(_x)
            modified_count += 1

    def sort_key(list_item: dict[str, int | str | None]) -> int:
        k = list_item[SEC.SecFeed.feed_id.key]
        assert isinstance(k, int)
        return k

    result.sort(key=sort_key)
    return result


def get_feed_info(
    modelXbrl: ModelXbrl, last_modified_date: datetime.datetime | None
) -> tuple[dict[str, Any], ModelDocument]:
    """Gets feed info ready to insert in db"""
    cntlr = modelXbrl.modelManager.cntlr
    modelDoc = modelXbrl.modelDocument
    rss_items: list[ModelRssItem] = getattr(modelDoc, "rssItems", [])
    assert isinstance(rss_items, list)
    assert isinstance(modelDoc, ModelDocument)
    feed_dict = dict.fromkeys(getattr(SEC.SecFeed, "cols_names", lambda: [])())

    _x = feed_dict.pop(
        SEC.SecFeed.created_updated_at.key, None
    )  # timestamp auto generated
    del _x
    # feed id based on feed year-month, except for if latest 200,
    # figure feed_id and last_modified_date based on lastBuildDate
    # last_modified_date is irrelevant in case of last 200 because
    # feed table is not updated
    feed_month = None
    feed_month_full_date: datetime.datetime | None = None

    if modelDoc.basename == os.path.basename(
        constants.RSS_FEEDS["US SEC All Filings"]
    ):  # latest 200
        feed_month_full_date = last_modified_date = parseRfcDatetime(
            modelDoc.xmlRootElement.xpath("./channel/lastBuildDate")[
                0
            ].textValue
        )
        assert isinstance(feed_month_full_date, datetime.datetime)
        feed_month = datetime.datetime.strftime(feed_month_full_date, "%Y-%m")
    else:
        file_id_pattern = file_id_re.search(modelDoc.basename)
        assert file_id_pattern is not None
        feed_month = file_id_pattern.group()
        _feed_month_full_date = parser.parse(feed_month)
        feed_month_full_date = _feed_month_full_date.replace(
            day=monthrange(
                year=_feed_month_full_date.year,
                month=_feed_month_full_date.month,
            )[1]
        )
        if not feed_month:
            cntlr.addToLog(
                (f"Don't know what to do with {modelDoc.basename}"),
                **log_template("error", cntlr, logging.ERROR),
            )
            raise XIDBException(
                f"Don't know what to do with {modelDoc.basename}"
            )

    feed_dict[SEC.SecFeed.feed_id.key] = int(feed_month.replace("-", ""))
    feed_dict[SEC.SecFeed.feed_month.key] = feed_month_full_date.date()
    feed_objects = modelDoc.xmlDocument.xpath(".//channel/*[not(self::item)]")
    # retrieve feed header info (remodify tag names to db)
    for inf in feed_objects:
        tag = str(inf.qname)
        val = None
        if tag.startswith("atom:"):
            val = inf.attr("href")
            tag = "feed_link"
        elif tag in ("pubDate", "lastBuildDate") and inf.text is not None:
            try:
                val = parser.parse(
                    inf.text, tzinfos={"EST": "UTC-5:00", "EDT": "UTC-4:00"}
                )
            except Exception:
                pass
        else:
            val = inf.text
        tag = replace_caps(tag)
        feed_dict[tag] = val
    # add last modified date retrieved from links list
    feed_dict[SEC.SecFeed.last_modified_date.key] = last_modified_date
    feed_dict[SEC.SecFeed.included_filings_count.key] = len(rss_items)
    xmlRoot = getattr(modelXbrl.modelDocument, "xmlRootElement", None)
    assert isinstance(xmlRoot, etree._Element)
    files_list = xmlRoot.xpath('//*[local-name()="xbrlFile"]')
    assert isinstance(files_list, list)
    feed_dict[SEC.SecFeed.included_files_count.key] = len(files_list)
    # first published first in db
    rss_items.sort(key=lambda x: cast(datetime.datetime, x.pubDate))
    return feed_dict, modelDoc


def get_files_info(
    rssItem: ModelRssItem, feed_id: int, filing_id: int
) -> list[dict[str, Any]]:
    """Get filing files info from rss item"""
    _i = rssItem
    files_list = []
    item_files = XmlUtil.descendants(_i, _i.edgr, "xbrlFile")
    file_id = 1
    for _f in item_files:
        file_dict = dict.fromkeys(
            getattr(SEC.SecFile, "cols_names", lambda: [])()
        )
        _x = file_dict.pop(
            SEC.SecFile.created_updated_at.key, None
        )  # timestamp auto generated
        del _x
        file_sequence = (
            int(_f.get(_i.edgrSequence))
            if _f.get(_i.edgrSequence)
            else file_id
        )
        file_dict[SEC.SecFile.filing_id.key] = filing_id
        file_dict[SEC.SecFile.feed_id.key] = feed_id
        file_dict[SEC.SecFile.file_id.key] = int(
            str(filing_id) + str(file_sequence).zfill(3)
        )
        file_dict[SEC.SecFile.sequence.key] = file_sequence
        file_dict[SEC.SecFile.file.key] = _f.get(_i.edgrFile)
        file_dict[SEC.SecFile.type.key] = _f.get(_i.edgrType)
        file_dict[SEC.SecFile.size.key] = (
            int(_f.get("{https://www.sec.gov/Archives/edgar}size"))
            if _f.get("{https://www.sec.gov/Archives/edgar}size")
            else None
        )
        file_dict[SEC.SecFile.description.key] = _f.get(_i.edgrDescription)
        file_dict[SEC.SecFile.url.key] = _f.get(_i.edgrUrl)
        file_dict[SEC.SecFile.accession_number.key] = getattr(
            _i, "accessionNumber", None
        )
        file_dict[SEC.SecFile.duplicate.key] = 0
        file_dict[SEC.SecFile.inline_xbrl.key] = int(
            _f.get(_i.edgrInlineXBRL) == XbrlConst.booleanValueTrue
        )
        tags_dict = {
            "ins": "INS",
            "sch": "SCH",
            "cal": "CAL",
            "def": "DEF",
            "lab": "LAB",
            "pre": "PRE",
        }
        file_type = file_dict.get("type")
        if isinstance(file_type, str) and len(file_type) >= 3:
            file_dict[SEC.SecFile.type_tag.key] = tags_dict.get(
                file_type[-3:].lower()
            )
        if not file_dict[SEC.SecFile.type_tag.key]:
            file_dict[SEC.SecFile.type_tag.key] = (
                "INS" if file_dict[SEC.SecFile.inline_xbrl.key] else "OTHER"
            )
        files_list.append(file_dict)
        file_id += 1
    return files_list


def get_filing_info(
    rss_item: ModelRssItem, feed_id: int, filing_id: int
) -> dict[str, Any]:
    """Get filing information from rss item"""
    _i = rss_item
    item_dict = dict.fromkeys(
        getattr(SEC.SecFiling, "cols_names", lambda: [])()
    )
    _x = item_dict.pop(
        SEC.SecFiling.created_updated_at.key, None
    )  # timestamp auto generated
    del _x
    item_dict[SEC.SecFiling.feed_id.key] = feed_id
    item_dict[SEC.SecFiling.filing_id.key] = filing_id
    item_dict[SEC.SecFiling.inline_xbrl.key] = 0
    item_dict[SEC.SecFiling.duplicate.key] = 0
    inline_attr = _i.xpath('.//@*[local-name()="inlineXBRL"]')
    assert isinstance(inline_attr, list)
    if inline_attr:
        if inline_attr[0] == XbrlConst.booleanValueTrue:
            item_dict[SEC.SecFiling.inline_xbrl.key] = 1
    item_title = _i.find("title")
    if item_title is not None:
        item_dict[SEC.SecFiling.filing_title.key] = item_title.text
    item_description = _i.find("description")
    if item_description is not None:
        item_dict[SEC.SecFiling.filing_description.key] = item_description.text
    link_elt = _i.find("link")
    if isinstance(link_elt, etree._Element):
        item_dict[SEC.SecFiling.filing_link.key] = link_elt.text
    item_dict[SEC.SecFiling.primary_document_url.key] = _i.primaryDocumentURL
    # all_target_attrs = [
    #     'enclosureUrl', 'enclosureSize', 'url', 'pubDate',
    #     'companyName', 'formType',
    #     'filingDate', 'cikNumber', 'accessionNumber', 'fileNumber',
    #     'acceptanceDatetime', 'period', 'assignedSic', 'fiscalYearEnd'
    # ]
    item_dict[SEC.SecFiling.enclosure_url.key] = getattr(
        _i, "enclosureUrl", None
    )
    item_dict[SEC.SecFiling.entry_point.key] = getattr(_i, "url", None)
    item_dict[SEC.SecFiling.pub_date.key] = getattr(_i, "pubDate", None)
    item_dict[SEC.SecFiling.company_name.key] = getattr(
        _i, "companyName", None
    )
    item_dict[SEC.SecFiling.form_type.key] = getattr(_i, "formType", None)
    item_dict[SEC.SecFiling.cik_number.key] = getattr(_i, "cikNumber", None)
    item_dict[SEC.SecFiling.accession_number.key] = getattr(
        _i, "accessionNumber", None
    )
    item_dict[SEC.SecFiling.file_number.key] = getattr(_i, "fileNumber", None)
    item_dict[SEC.SecFiling.acceptance_datetime.key] = getattr(
        _i, "acceptanceDatetime", None
    )
    # Catch up
    if _i.find("enclosure") is not None:
        enclosure_elt = _i.find("enclosure")
        if isinstance(enclosure_elt, etree._Element):
            enclosure_length: int | str | None = enclosure_elt.get("length")
            if isinstance(enclosure_length, str) and len(enclosure_length) > 0:
                enclosure_length = int(enclosure_length)
            item_dict[SEC.SecFiling.enclosure_size.key] = enclosure_length
    filing_date: Any = getattr(_i, "filingDate", None)
    if isinstance(filing_date, datetime.date):
        filing_date = datetime.datetime.combine(
            filing_date, datetime.datetime.min.time()
        )
    item_dict[SEC.SecFiling.filing_date.key] = filing_date

    item_dict[SEC.SecFiling.assigned_sic.key] = 0
    assigned_sic: Any = getattr(_i, "assignedSic", False)
    if isinstance(assigned_sic, str) and len(assigned_sic) > 0:
        item_dict[SEC.SecFiling.assigned_sic.key] = int(assigned_sic)
    _period: Any = getattr(_i, "period", False)
    if isinstance(_period, str):
        item_dict[SEC.SecFiling.period.key] = parser.parse(_period)
    _director: Any = _i.xpath('.//*[local-name()="assistantDirector"]/text()')
    if isinstance(_director, list) and len(_director) > 0:
        item_dict[SEC.SecFiling.assistant_director.key] = _director[0]

    if getattr(_i, "fiscalYearEnd", None):
        fiscal_year_end = getattr(_i, "fiscalYearEnd", None)
        if isinstance(fiscal_year_end, str):
            item_dict[SEC.SecFiling.fiscal_year_end.key] = fiscal_year_end
            month_day = fiscal_year_end.split("-")
            if len(month_day) > 1:
                item_dict[SEC.SecFiling.fiscal_year_end_month.key] = int(
                    month_day[0]
                )
                item_dict[SEC.SecFiling.fiscal_year_end_day.key] = int(
                    month_day[1]
                )

    return item_dict


def get_new_or_modified_filings(
    engine: Engine, feed_id: int, modelDoc: ModelDocument, is_modified: bool
) -> list[ModelRssItem]:
    """Returns a list of rss items to process after excluding existing items"""
    rss_items: list[ModelRssItem] = getattr(modelDoc, "rssItems", [])
    is_latest = modelDoc.uri == constants.LATEST_FEEDS_URL
    if not is_modified and not is_latest:
        return rss_items

    existing_filings: list[tuple[Any, Any, Any, Any, Any]] = []
    with Session(engine) as session:
        db_feed = session.query(SEC.SecFiling)
        if is_latest:
            last_feed = session.query(func.max(SEC.SecFeed.feed_id)).scalar()
            db_feed = db_feed.where(SEC.SecFiling.feed_id == last_feed)
        else:
            db_feed = db_feed.where(SEC.SecFiling.feed_id == feed_id)
        existing_filings = db_feed.with_entities(
            SEC.SecFiling.accession_number,
            SEC.SecFiling.enclosure_url,
            SEC.SecFiling.acceptance_datetime,
            SEC.SecFiling.pub_date,
            SEC.SecFiling.cik_number,
        ).all()
    # check which we need to insert, detected changes are inserted as
    # new then previously existing same filings with same accession
    # number are marked as duplicates
    existing_filings_set = set(existing_filings)
    discovered_filings_set: set[tuple[Any, Any, Any, Any, Any]] = {
        (
            _i.accessionNumber,
            _i.enclosureUrl,
            _i.acceptanceDatetime,
            _i.pubDate,
            _i.cikNumber,
        )
        for _i in rss_items
    }
    new_or_changed_filings_set = discovered_filings_set - existing_filings_set
    new_or_changed_filings_accessions = {
        x[0] for x in new_or_changed_filings_set
    }
    result = [
        x
        for x in rss_items
        if x.accessionNumber in new_or_changed_filings_accessions
    ]
    result.sort(key=lambda x: cast(datetime.datetime, x.pubDate))
    return result


def get_new_or_modified_filers(
    filings_to_insert: list[Any], engine: Engine
) -> tuple[set[Any], set[Any]]:
    """Gets new and modified filers from the current batch to insert"""
    discovered_ciks_not_in_db = set()
    detected_changes_ciks = set()

    discovered_ciks = {
        x[SEC.SecFiling.cik_number.key]: x[SEC.SecFiling.company_name.key]
        for x in filings_to_insert
    }
    with Session(engine) as session:
        db_ciks = (
            session.query(SEC.SecFiler.cik_number, SEC.SecFiler.conformed_name)
            .where(SEC.SecFiler.cik_number.in_(discovered_ciks.keys()))
            .all()
        )
    discovered_ciks_not_in_db.update(
        set(discovered_ciks.keys()) - {x[0] for x in db_ciks}
    )
    for cik, name in db_ciks:
        if discovered_ciks[cik] != name:
            detected_changes_ciks.add(cik)
    return detected_changes_ciks, discovered_ciks_not_in_db


def get_filer_information(
    cntlr: Cntlr, cik: str, timeout: int = 1, url: str | None = None
) -> dict[str, Any]:
    """Download and parse filer information"""

    if url is None:
        url = (
            f"https://www.sec.gov/cgi-bin/browse-edgar?CIK="
            f"{cik}&action=getcompany&output=atom"
        )

    filer_information = {}

    resp = cntlr.webCache.opener.open(url, timeout=timeout)
    tree = etree.parse(resp)
    root = tree.getroot()
    _ns: dict[Any, Any] = root.nsmap
    filer = root.find(".//company-info", namespaces=_ns)
    assert isinstance(filer, etree._Element)
    adder_type = ["mailing", "business"]
    addr_info = ["city", "state", "zip"]
    addresses = []
    for addr in adder_type:
        _addr_elt = filer.find(f'./addresses/address[@type="{addr}"]', _ns)
        if isinstance(_addr_elt, etree._Element):
            address_values = [
                getattr(_addr_elt.find(f"./{x}", _ns), "text", None)
                for x in addr_info
            ]
            address_keys = [f"{addr}_{x}" for x in addr_info]
            addresses.append(dict(zip(address_keys, address_values)))

    former_names_elt = filer.find("./formerly-names", _ns)
    former_names = []
    if former_names_elt is not None:
        _former_names = [
            {
                SEC.SecFormerNames.cik_number.key: cik,
                SEC.SecFormerNames.name.key: x.text,
                SEC.SecFormerNames.date_changed.key: parser.parse(
                    y.text
                ).date()
                if isinstance(y.text, str)
                else None,
            }
            for x, y in zip(
                former_names_elt.findall(".//name", _ns),
                former_names_elt.findall(".//date", _ns),
            )
        ]
        # last name change first
        former_names = sorted(
            _former_names,
            key=lambda x: cast(
                datetime.datetime, x[SEC.SecFormerNames.date_changed.key]
            ),
            reverse=True,
        )

    _filer_information = {
        SEC.SecFiler.conformed_name.key: getattr(
            filer.find("./conformed-name", _ns), "text", None
        ),
        SEC.SecFiler.cik_number.key: getattr(
            filer.find("./cik", _ns), "text", None
        ),
        SEC.SecFiler.industry_code.key: getattr(
            filer.find("./assigned-sic", _ns), "text", None
        ),
        SEC.SecFiler.industry_description.key: getattr(
            filer.find("./assigned-sic-desc", _ns), "text", None
        ),
        SEC.SecFiler.state_of_incorporation.key: getattr(
            filer.find("./state-of-incorporation", _ns), "text", None
        ),
        SEC.SecFiler.country.key: None,
        SEC.SecFiler.former_names.key: former_names if former_names else [],
        **addresses[0],
        **addresses[1],
    }
    for k, v in _filer_information.items():
        filer_information[k] = v.strip() if isinstance(v, str) else v
    location_code: str = "XX"
    if filer_information[SEC.SecFiler.business_state.key]:
        location_code = str(filer_information[SEC.SecFiler.business_state.key])
    elif filer_information[SEC.SecFiler.mailing_city.key]:
        location_code = str(filer_information[SEC.SecFiler.mailing_state.key])
    filer_information[SEC.SecFiler.country.key] = constants.STATE_CODES.get(
        location_code.upper(), constants.STATE_CODES["XX"]
    )[0]

    filer_information[SEC.SecFiler.location_code.key] = location_code

    return {"filer": filer_information, "cik": cik}


def get_sec_cik_ticker_mapping(cntlr: Cntlr) -> list[dict[str, Any]]:
    """Get sec cik ticker mapping from
    https://www.sec.gov/files/company_tickers_exchange.json
    """
    start_at = time.perf_counter()
    result = []
    url = "https://www.sec.gov/files/company_tickers_exchange.json"
    filename = cntlr.webCache.getfilename(url, reload=True)
    if filename:
        cik_mapping_file = None
        with open(filename, "r", encoding="utf-8") as cik_tkr_f:
            cik_mapping_file = json.load(cik_tkr_f)
        cik_mapping_data = cik_mapping_file["data"]
        for cik_tkr in cik_mapping_data:
            result.append(
                {
                    str(SEC.SecCikTickerMapping.cik_number.key): str(
                        cik_tkr[0]
                    ).zfill(10),
                    str(SEC.SecCikTickerMapping.company_name.key): cik_tkr[1],
                    str(SEC.SecCikTickerMapping.ticker_symbol.key): cik_tkr[2],
                    str(SEC.SecCikTickerMapping.exchange.key): cik_tkr[3],
                }
            )
        # update stored
        store_filename = os.path.join(
            getattr(cntlr, "db_cache_dir"),
            getattr(SEC.SecCikTickerMapping, "__tablename__", "")
            + "-data.pkl",
        )
        with open(store_filename, "wb") as pkl:
            pickle.dump(result, pkl)
        time_taken = get_time_elapsed(start_at)

        msg = (
            f"Retrieved SEC cik ticker mapping and updated cache "
            f"({store_filename}) in {time_taken} sec(s)"
        )
        cntlr.addToLog(msg, **log_template("info", cntlr, logging.ERROR))
    else:
        msg = f"Could not get data from {url}."
        cntlr.addToLog(msg, **log_template("error", cntlr, logging.ERROR))
        raise Exception(msg)
    return result


def get_esef_errors(
    filing_dict: dict[str, Any]
) -> tuple[list[dict[str, str]], bool, str | None]:
    """Extracts filing errors from ESEF XBRL filings index"""
    esef_error = []
    load_error = None
    is_loadable = True
    filing_errors = filing_dict["errors"]
    if len(filing_errors):
        for _error in filing_errors:
            error_code = _error["code"]
            _error_code_split = (
                error_code.split(":")[-1] if error_code else None
            )
            if is_loadable and _error_code_split in constants.ESEF_LOAD_ERRORS:
                is_loadable = False
                load_error = error_code
            error = {
                str(ESEF.EsefFilingError.severity.key): _error["sev"],
                str(ESEF.EsefFilingError.code.key): _error["code"],
                str(ESEF.EsefFilingError.message.key): _error["msg"],
            }
            esef_error.append(error)
    return esef_error, is_loadable, load_error


def get_esef_filing(
    filing_addr: str,
    filing_dict: dict[str, Any],
    is_loadable: bool,
    load_error: str | None,
) -> dict[str, int | str | datetime.datetime | datetime.date | None]:
    """Extracts filing information from ESEF XBRL filing index"""
    esef_filing: dict[
        str, int | str | datetime.datetime | datetime.date | None
    ] = {}
    esef_filing[str(ESEF.EsefFiling.filing_key.key)] = filing_addr
    esef_filing[str(ESEF.EsefFiling.filing_root.key)] = filing_addr.rsplit(
        "/", 1
    )[0]
    esef_filing[str(ESEF.EsefFiling.filing_number.key)] = int(
        filing_addr.rsplit("/", 1)[-1]
    )
    esef_filing[str(ESEF.EsefFiling.entity_lei.key)] = filing_dict.get(
        "lei", None
    )
    esef_filing[str(ESEF.EsefFiling.country.key)] = filing_dict.get(
        "country", None
    )
    esef_filing[str(ESEF.EsefFiling.filing_system.key)] = filing_dict.get(
        "system", None
    )
    if filing_dict.get("added", False):
        date_added = parser.parse(filing_dict.get("added", False))
    esef_filing[str(ESEF.EsefFiling.date_added.key)] = date_added.date()
    if filing_dict.get("date", False):
        report_date = parser.parse(filing_dict["date"])
    esef_filing[str(ESEF.EsefFiling.report_date.key)] = report_date.date()
    esef_filing[str(ESEF.EsefFiling.xbrl_json_instance.key)] = filing_dict.get(
        "xbrl-json", None
    )
    esef_filing[str(ESEF.EsefFiling.report_package.key)] = filing_dict.get(
        "report-package", None
    )
    esef_filing[str(ESEF.EsefFiling.viewer_document.key)] = filing_dict.get(
        "viewer", None
    )
    esef_filing[str(ESEF.EsefFiling.report_document.key)] = filing_dict.get(
        "report", None
    )
    esef_filing[str(ESEF.EsefFiling.is_loadable.key)] = is_loadable
    esef_filing[str(ESEF.EsefFiling.load_error.key)] = load_error
    return esef_filing


def infer_esef_filing_language(
    cntlr: Cntlr, xbrl_json_instance_link: str
) -> list[dict[str, Any]]:
    """Reads in json instance and gets language used per fact if available."""
    is_file = xbrl_json_instance_link.startswith("file://")
    result: list[dict[str, Any]] = []
    resp = cntlr.webCache.opener.open(xbrl_json_instance_link, timeout=5)
    if resp.code == 200 or is_file:
        resp_data = json.loads(resp.read().decode())
        counts: defaultdict[str, int] = defaultdict(int)
        instance_facts = resp_data["facts"]
        facts_in_report = len(instance_facts)
        for _fct_id, fct in instance_facts.items():
            lang = fct.get("dimensions", False).get("language", False)
            if lang:
                counts[lang] += 1
        for _lang, _count in counts.items():
            row = {
                str(ESEF.EsefInferredFilingLanguage.lang.key): _lang,
                str(ESEF.EsefInferredFilingLanguage.lang_name.key): getattr(
                    languages.get(alpha_2=_lang), "name", None
                ),
                str(ESEF.EsefInferredFilingLanguage.facts_in_lang.key): _count,
                str(
                    ESEF.EsefInferredFilingLanguage.facts_in_report.key
                ): facts_in_report,
            }
            result.append(row)
        del resp_data, instance_facts
    return result


def get_esef_filing_langs(
    filing_dict: dict[str, Any]
) -> list[dict[str, str | None]]:
    """Gets filing langs as shown in ESEF XBRL filings index"""
    esef_filing_langs: list[dict[str, str | None]] = []
    filing_langs = filing_dict.get("langs", False)
    if filing_langs:
        for lang in filing_langs:
            filing_lang = {}
            filing_lang[str(ESEF.EsefFilingLang.lang.key)] = lang
            filing_lang[str(ESEF.EsefFilingLang.lang_name.key)] = getattr(
                languages.get(alpha_2=lang), "name", None
            )
            esef_filing_langs.append(filing_lang)
    return esef_filing_langs


def get_esef_all_filing_info(
    filing_addr: str, filing_dict: dict[str, Any]
) -> tuple[dict[str, Any], list[Any], list[Any]]:
    """Grouping all functions that extract ESEF filing information
    for each table"""
    filing = filing_dict.copy()
    esef_filing: dict[str, Any] = {}
    esef_error: list[Any] = []
    esef_filing_langs: list[dict[str, str | None]] = []
    is_loadable: bool = True
    load_error: str | None = None
    esef_filing_langs = get_esef_filing_langs(filing)
    esef_error, is_loadable, load_error = get_esef_errors(filing)
    esef_filing = get_esef_filing(filing_addr, filing, is_loadable, load_error)
    return esef_filing, esef_error, esef_filing_langs


def extract_esef_entity_lei_info(
    cntlr: Cntlr,
    entity_lei_data: dict[str, Any],
    is_test: bool = False,
    test_data_url: pathlib.Path | None = None,
) -> tuple[dict[str, Any], list[Any]]:
    """Extract entity information from data retrieved from lei api"""
    esef_entity: dict[str, Any] = {}
    esef_other_names_list: list[Any] = []
    lei: str = entity_lei_data["attributes"]["lei"]
    entity_data: dict[str, Any] = entity_lei_data["attributes"]["entity"]
    esef_entity[str(ESEF.EsefEntity.entity_lei.key)] = lei
    esef_entity[str(ESEF.EsefEntity.lei_legal_name.key)] = entity_data.get(
        "legalName", {}
    ).get("name", None)
    esef_entity[str(ESEF.EsefEntity.lei_legal_address_lines.key)] = " ".join(
        entity_data.get("legalAddress", {}).get("addressLines", [])
    )
    esef_entity[
        str(ESEF.EsefEntity.lei_legal_address_city.key)
    ] = entity_data.get("legalAddress", {}).get("city", None)
    esef_entity[
        str(ESEF.EsefEntity.lei_legal_address_country.key)
    ] = entity_data.get("legalAddress", {}).get("country", None)
    esef_entity[
        str(ESEF.EsefEntity.location_code.key)
    ] = constants.state_codes_by_alpha_2[
        entity_data.get("legalAddress", {}).get("country", None)
    ]
    esef_entity[
        str(ESEF.EsefEntity.lei_legal_address_postal_code.key)
    ] = entity_data.get("legalAddress", {}).get("postalCode", None)
    esef_entity[str(ESEF.EsefEntity.lei_hq_address_lines.key)] = " ".join(
        entity_data.get("headquartersAddress", {}).get("addressLines", [])
    )
    esef_entity[
        str(ESEF.EsefEntity.lei_hq_address_city.key)
    ] = entity_data.get("headquartersAddress", {}).get("city", None)
    esef_entity[
        str(ESEF.EsefEntity.lei_hq_address_country.key)
    ] = entity_data.get("headquartersAddress", {}).get("country", None)
    esef_entity[
        str(ESEF.EsefEntity.lei_hq_address_postal_code.key)
    ] = entity_data.get("headquartersAddress", {}).get("postalCode", None)
    esef_entity[str(ESEF.EsefEntity.lei_category.key)] = entity_data.get(
        "category", None
    )
    lei_other_names = entity_data.get("otherNames", [])
    if lei_other_names:
        unique_other_names = {
            (x.get("name", "UNKNOWN"), x.get("type", "UNKNOWN"))
            for x in lei_other_names
        }
        for other_name in unique_other_names:
            esef_other_name = {}
            esef_other_name[ESEF.EsefEntityOtherName.entity_lei.key] = lei
            esef_other_name[
                ESEF.EsefEntityOtherName.other_name.key
            ] = other_name[0]
            esef_other_name[
                ESEF.EsefEntityOtherName.other_name_type.key
            ] = other_name[1]
            esef_other_names_list.append(esef_other_name)
    isin_uri = f"https://api.gleif.org/api/v1/lei-records/{lei}/isins"
    if is_test and isinstance(test_data_url, pathlib.Path):
        isin_uri = test_data_url.as_uri() + f"/esef/isin/{lei}"
    isin_resp = cntlr.webCache.opener.open(isin_uri)
    isin_data = json.loads(isin_resp.read().decode())
    if isin_data.get("data", False):
        isin = ",".join(
            [
                isin.get("attributes", {}).get("isin", None)
                for isin in isin_data["data"]
            ]
        )
        esef_entity[str(ESEF.EsefEntity.lei_isin.key)] = isin
    return esef_entity, esef_other_names_list


def get_esef_filings_index(
    cntlr: Cntlr, reload_cache: bool = True, loc: str | None = None
) -> tuple[str, dict[str, Any]]:
    """Down loads ESEF XBRL filings index json file"""
    base_url: str = constants.FILINGS_XBRL_ORG
    link: str = urljoin(base_url, "index.json")
    if isinstance(loc, str) and loc.startswith("file://"):
        link = loc
        if sys.platform == "win32" and loc.startswith("file:///"):
            link = loc.replace("file:///", "")
    file = cntlr.webCache.getfilename(link, reload=reload_cache)
    esef_index: dict[str, Any]
    if isinstance(file, str) and os.path.exists(file):
        with open(file, "r", encoding="utf-8") as esf:
            esef_index = json.load(esf)
    return base_url, esef_index


def get_sp_100(cntlr: Cntlr) -> list[str]:
    """Get sp 100 companies tickers from wikipedia"""
    url = "https://en.wikipedia.org/wiki/S%26P_100"
    sp100_resp = cntlr.webCache.opener.open(url)
    tree = html.parse(sp100_resp)
    root = tree.getroot()
    trs = root.xpath('.//table[@id="constituents"]//tbody/tr')
    sp_100_tkrs = []
    for _tr in trs:
        sp_100_tkrs.append(
            tuple(
                d.text.replace("\n", "")
                if d.text
                else d.xpath(".//a/text()")[0]
                for d in _tr.findall("td")
            )
        )

    tks_site = [x[0].lower().replace(".", "-") for x in sp_100_tkrs if x]
    tks_site.sort()
    return list(set(tks_site))


def get_sp_500(cntlr: Cntlr) -> list[tuple[Any, Any, Any]]:
    """Get sp 500 companies tickers and ciks from wikipedia"""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    sp500_resp = cntlr.webCache.opener.open(url)
    tree = html.parse(sp500_resp)
    root = tree.getroot()
    trs = root.xpath('//table[@id="constituents"]/tbody/tr')
    sp_500_tkrs = []
    for _tr in trs:
        sp_500_tkrs.append(
            tuple(
                d.text.replace("\n", "")
                if d.text
                else (
                    d.xpath(".//a/text()")[0]
                    if d.xpath(".//a/text()")
                    else None
                )
                for d in _tr.findall("td")
            )
        )

    tks_site: list[tuple[Any, Any, Any]] = [
        (x[0].lower().replace(".", "-"), x[-2], x[-3])
        for x in sp_500_tkrs
        if x
    ]
    res = list(set(tks_site))
    res.sort(key=lambda x: cast(str, x[0]))
    return res


def get_sp_companies_ciks(cntlr: Cntlr) -> list[dict[str, Any]]:
    """returns (
        (todays_date, cikNumber, in_sp100, ticker_symbol, date_first_added),
        ) based on:
    https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
    https://en.wikipedia.org/wiki/S%26P_100
    """
    start_at = time.perf_counter()
    sp500 = get_sp_500(cntlr)
    sp100 = get_sp_100(cntlr)

    sp500_dict_by_tkr = dict((tkr, (cik, xdate)) for tkr, cik, xdate in sp500)
    sp500_dict_by_cik = {}
    for tkr, cik, add_date in sp500:
        sp500_dict_by_cik[cik] = (tkr, add_date)
    sp100_dict_by_cik = defaultdict(set)
    for tkr2 in sp100:
        sp100_dict_by_cik[sp500_dict_by_tkr[tkr2][0]].add(tkr2)

    result = [
        {
            str(
                SEC.SpCompaniesCiks.as_of_date.key
            ): datetime.datetime.today().date(),
            str(SEC.SpCompaniesCiks.cik_number.key): cik,
            str(SEC.SpCompaniesCiks.is_sp100.key): cik in sp100_dict_by_cik,
            str(SEC.SpCompaniesCiks.ticker_symbol.key): tkr_date[0],
            str(SEC.SpCompaniesCiks.date_first_added.key): tkr_date[1],
        }
        for cik, tkr_date in sp500_dict_by_cik.items()
    ]

    store_filename = os.path.join(
        getattr(cntlr, "db_cache_dir"),
        getattr(SEC.SpCompaniesCiks, "__tablename__", "") + "-data.pkl",
    )
    with open(store_filename, "wb") as pkl:
        pickle.dump(result, pkl)
    time_taken = get_time_elapsed(start_at)
    msg = (
        f"Retrieved S&P companies and updated cache ({store_filename})"
        f" in {time_taken} sec(s)"
    )
    cntlr.addToLog(msg, **log_template("info", cntlr, logging.ERROR))
    return result


def pickle_table_data(engine: Engine, table_model: Any) -> str:
    """Copy database table and pickle its date in cache folder"""
    store_filename: str = os.path.join(
        getattr(engine, "db_cache_dir"),
        table_model.__tablename__ + "-data.pkl",
    )
    with Session(engine) as session:
        table_data = session.query(table_model)
        table_data_dicts = [instance_to_dict(x) for x in table_data]
        with open(store_filename, "wb") as pkl:
            pickle.dump(table_data_dicts, pkl)
    return store_filename

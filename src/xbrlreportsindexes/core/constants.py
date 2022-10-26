"""Log messages, errors, links, task names, country codes ..."""
from __future__ import annotations

import logging
from collections import OrderedDict
from typing import Any
from urllib import request

from lxml import html
from xbrlreportsindexes.core.arelle_utils import RSS_DB_LOG_HANDLER_NAME
from xbrlreportsindexes.model import ESEF
from xbrlreportsindexes.model import SEC


try:
    from arelle.DialogRssWatch import rssFeeds
    from arelle.Cntlr import Cntlr
    from arelle.plugin.xbrlDB.SqlDb import XPDBException
except ModuleNotFoundError as exc:
    raise Exception("Please add path to arelle to python path") from exc

LOCAL_CACHE_DIR_NAME: str = "index-db-cache"
MOCK_TEST_DATA_DIR_NAME: str = "mock_test_data"
LOG_HANDLER_NAME: str = RSS_DB_LOG_HANDLER_NAME
RSS_FEEDS: dict[str, str] = rssFeeds
MONTHLY_FEEDS_URL: str = "https://www.sec.gov/Archives/edgar/monthly/"
FILINGS_XBRL_ORG: str = "https://filings.xbrl.org/"
LATEST_FEEDS_URL: str = "https://www.sec.gov/Archives/edgar/xbrlrss.all.xml"
RSS_DB_PREFIX: str = "rss-db"
MSG_WHILE_SAVED: str = (
    "This may take some time, press Ctrl+C to cancel, "
    "progress will be saved..."
)
TSK_INITIALIZE_DB: str = "initialize-db"
TSK_GET_FEEDS: str = "get-feeds"
TSK_NEW_OR_MODIFIED_FEEDS: str = "new-or-modified-feeds"
TSK_NEW_OR_MODIFIED_FILINGS: str = "new-or-modified-filings"
TSK_GET_FEED_DATA: str = "get-feed-data"
TSK_FEED_DB_INSERT: str = "insert-feed-data"
TSK_INSERT_TABLE: str = "insert-table"
TSK_DELETE_TABLE: str = "delete-table"
TSK_PROCESS_FEEDS: str = "process-feeds"
DB_UPDATE_FEEDS: str = "update-feeds"
DB_INSERT_NEW_FILERS: str = "insert-new-filers"
DB_UPDATE_EXISTING_FILERS: str = "update-existing-filers"
DB_REFRESH_TABLES: str = "refresh-tables"
DB_PROCESS_DUPLICATES: str = "process-duplicates"
DB_GET_EXISTING_ESEF_INFO: str = "get-existing-esef-info"
TSK_ESEF_INDEX: str = "get-esef-index"
DB_UPDATE_ESEF_FILINGS: str = "update-esef-filings"
DB_UPDATE_ESEF_ENTITIES: str = "update-esef-entities"

TASK_LOG_KEYS: dict[str, tuple[str, ...]] = {
    TSK_INITIALIZE_DB: ("time",),
    TSK_GET_FEEDS: ("time", "count"),
    TSK_NEW_OR_MODIFIED_FEEDS: ("time", "count"),
    TSK_NEW_OR_MODIFIED_FILINGS: ("time", "count", "feed_id"),
    TSK_GET_FEED_DATA: ("time", "count", "feed_id"),
    TSK_FEED_DB_INSERT: ("time", "stats"),
    TSK_INSERT_TABLE: ("time", "stats"),
    TSK_DELETE_TABLE: ("time", "stats"),
    TSK_PROCESS_FEEDS: ("time",),
    DB_UPDATE_FEEDS: ("time", "count"),
    DB_INSERT_NEW_FILERS: ("time", "count"),
    DB_UPDATE_EXISTING_FILERS: ("time", "count"),
    DB_REFRESH_TABLES: ("time", "count"),
    DB_GET_EXISTING_ESEF_INFO: ("time",),
    TSK_ESEF_INDEX: ("time",),
    DB_UPDATE_ESEF_FILINGS: ("time", "count"),
    DB_UPDATE_ESEF_ENTITIES: ("time", "count"),
}

ERR_BAD_DATE_RANGE: str = "bad-date-range"
ERR_UNKNOWN_ACTION: str = "unknown-action"
ERR_MISSING_DATA: str = "missing-data"
ERR_DONT_KNOW: str = "dont-know"
ERR_NO_DB: str = "database-not-found"
ERR_FEED_NOT_LOADED: str = "database-not-found"
ERR_EXISTING_TASK_IN_PROGRESS: str = "existing-task-in-progress"
ERR_BOTH_MAIN_AND_CHILD: str = "main-and-child"
ERR_NO_TSK_ID: str = "invalid-task-id"
ERR_NO_TRACKER: str = "no-task-tracker"
ERR_BAD_DATE: str = "bad-date-format"
ERR_BAD_SEARCH_PARAM: str = "bad-search-parameter"
ERR_BAD_CONN_PARAM: str = "bad-connection-parameters"
ERR_BAD_CMD_PARAM: str = "bad-cmd-parameters"
ERR_BAD_TYPE: str = "bad-type"
ERR_NO_CNTLR: str = "cntlr-not-found"

ERROR_CODE_KEYS: dict[str, str] = {
    ERR_BAD_DATE_RANGE: '"To Date" and "From date" must be valid "yyyy-mm-dd"'
    "dates with to date later than from date.",
    ERR_UNKNOWN_ACTION: '"action" must be on of "insert" or "delete"',
    ERR_MISSING_DATA: '"data" must be a dict of row for "inset" action',
    ERR_DONT_KNOW: "Don't know what to do with the given args.",
    ERR_NO_DB: "Rss DB data base model does not exist",
    ERR_FEED_NOT_LOADED: "Could not load feed.",
    ERR_BOTH_MAIN_AND_CHILD: "Instance is marked as both parent and child",
    ERR_NO_TSK_ID: "No task was found for provided task id",
    ERR_EXISTING_TASK_IN_PROGRESS: "An existing task is already in progress.",
    ERR_NO_TRACKER: "Could not initialize task tracker",
    ERR_BAD_DATE: "Date must be in the format 2020-02-22",
}

ESEF_LOAD_ERRORS: set[str] = {
    "invalidDirectoryStructure",
    "metadataDirectoryNotFound",
    "invalidArchiveFormat",
    "invalidMetaDataFile",
    "IOerror",
    "FileNotFoundError",
}


def log_template(
    suffix: str, file: str | Cntlr | None, level: int = logging.INFO
) -> dict[str, Any]:
    """Convenience for quickly filling out needed params in log record"""
    return {
        "messageCode": f"{RSS_DB_PREFIX}.{suffix}",
        "file": str(file)
        if isinstance(file, (str, int))
        else getattr(file, "rss_dbname", ""),
        "level": level,
    }


def get_edgar_state_codes(
    get_location: bool = True,
) -> OrderedDict[str, tuple[Any]]:
    """Extracts Edgar state codes from
    https://www.sec.gov/edgar/searchedgar/edgarstatecodes.htm
    """
    url = "https://www.sec.gov/edgar/searchedgar/edgarstatecodes.htm"
    root = None
    with request.urlopen(url) as countries_resp:
        root = html.parse(countries_resp).getroot()
    table = root.xpath('.//th[text()="Code"]/ancestor::table[1]')[0]
    trs = table.findall(".//tr")
    titles = []
    sub_data = ""
    result = []
    for xtr in trs:
        children = xtr.getchildren()
        if all(x.tag == "th" for x in children):
            if len(children) > 1:
                titles = [x.text_content().strip() for x in children]
            if len(children) == 1:
                sub_data = children[0].text_content().strip()
        elif all(x.tag == "td" for x in children):
            data = [x.text.strip() for x in children]
            result.append({**dict(zip(titles, data)), "header": sub_data})

    lookup_dict = {
        "States": lambda x: ("US", x["State or Country Name"]),
        "Canadian Provinces": lambda x: ("CANADA", x["State or Country Name"]),
        "Other Countries": lambda x: (x.get("State or Country Name"), None),
    }

    codes: OrderedDict[str, Any] = OrderedDict()
    for _x in result:
        codes[_x["Code"]] = lookup_dict[_x["header"]](
            _x
        )  # type: ignore[no-untyped-call]
    # add location info
    if get_location:
        try:
            from geopy.geocoders import Nominatim  # type: ignore[import]

            geolocator = Nominatim(user_agent="testing")
            fixes = {
                # WASHINGTON
                "WA": ("US", "WASHINGTON STATE"),
                # CANADA (Federal Level)
                "Z4": ("CANADA", "CANADA"),
                # CONGO, THE DEMOCRATIC REPUBLIC OF THE
                "Y3": ("DEMOCRATIC REPUBLIC OF THE CONGO", None),
                "X4": ("VATICAN CITY STATE", None),
                # MOLDOVA, REPUBLIC OF
                "1S": ("MOLDOVA", None),
                # MICRONESIA, FEDERATED STATES OF
                "1K": ("FEDERATED STATES OF MICRONESIA", None),
                "M4": ("NORTH KOREA", None),
                "M5": ("SOUTH KOREA", None),
                "1U": ("MACEDONIA", None),
                "1X": ("PALESTINE", None),
                "K9": ("ISLAMIC REPUBLIC OF IRAN", None),
                "F5": ("TAIWAN", None),
                "W0": ("UNITED REPUBLIC OF TANZANIA", None),
                # 'UNITED STATES MINOR OUTLYING ISLANDS' reported as DC, US
                "2J": ("US", "DISTRICT OF COLUMBIA"),
                "D8": ("BRITISH VIRGIN ISLANDS", None),
                "VI": ("U.S. VIRGIN ISLANDS", None),
                # 'UNKNOWN' reported as DC, US
                "XX": ("US", "DISTRICT OF COLUMBIA"),
            }

            for _d in codes:
                _location = fixes[_d] if _d in fixes else codes[_d]
                if _location[0] == "US":
                    loc = geolocator.geocode(
                        _location[1] + ", " + _location[0], language="en"
                    )
                elif _location[0] == "CANADA":
                    loc = geolocator.geocode(_location[1], language="en")
                else:
                    loc = geolocator.geocode(_location[0], language="en")
                print(
                    loc,
                    " -- ",
                    " ".join(filter(None, _location)),
                    " -- ",
                    " ".join(filter(None, codes[_d])),
                )
                codes[_d] = (
                    codes[_d]
                    + (loc.latitude, loc.longitude)
                    + (
                        ", ".join(filter(None, (*reversed(_location),)))
                        if _d in fixes
                        else None,
                    )
                )  # location fix if any
        except Exception:
            pass
    return codes


class XIDBException(XPDBException):
    """Exception class for this project"""

    def __init__(
        self, code: str, msg: str | None = None, **kwargs: Any
    ) -> None:
        if msg is None:
            msg = ""
        else:
            msg = "\n" + msg
        message = ERROR_CODE_KEYS.get(code, "") + msg
        super().__init__(code, message, **kwargs)


# States/Countries codes data => country name, state name (US),
# latitude, longitude, location fix name (name used to get coordinates)
STATE_CODES: OrderedDict[
    str,
    tuple[
        str,
        str | None,
        float,
        float,
        str | None,
        str | None,
        str | None,
        str | None,
    ],
] = OrderedDict(
    [
        (
            "AL",
            (
                "US",
                "ALABAMA",
                33.2588817,
                -86.8295337,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "AK",
            (
                "US",
                "ALASKA",
                64.4459613,
                -149.680909,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "AZ",
            (
                "US",
                "ARIZONA",
                34.395342,
                -111.7632755,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "AR",
            (
                "US",
                "ARKANSAS",
                35.2048883,
                -92.4479108,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "CA",
            (
                "US",
                "CALIFORNIA",
                36.7014631,
                -118.7559974,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "CO",
            (
                "US",
                "COLORADO",
                38.7251776,
                -105.6077167,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "CT",
            (
                "US",
                "CONNECTICUT",
                41.6500201,
                -72.7342163,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "DE",
            (
                "US",
                "DELAWARE",
                38.6920451,
                -75.4013315,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "DC",
            (
                "US",
                "DISTRICT OF COLUMBIA",
                38.893661249999994,
                -76.98788325388196,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "FL",
            (
                "US",
                "FLORIDA",
                27.7567667,
                -81.4639835,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "GA",
            (
                "US",
                "GEORGIA",
                32.3293809,
                -83.1137366,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "HI",
            (
                "US",
                "HAWAII",
                19.58726775,
                -155.42688965312746,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "ID",
            (
                "US",
                "IDAHO",
                43.6447642,
                -114.0154071,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "IL",
            (
                "US",
                "ILLINOIS",
                40.0796606,
                -89.4337288,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "IN",
            (
                "US",
                "INDIANA",
                40.3270127,
                -86.1746933,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "IA",
            ("US", "IOWA", 41.9216734, -93.3122705, None, "US", "USA", "840"),
        ),
        (
            "KS",
            ("US", "KANSAS", 38.27312, -98.5821872, None, "US", "USA", "840"),
        ),
        (
            "KY",
            (
                "US",
                "KENTUCKY",
                37.5726028,
                -85.1551411,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "LA",
            (
                "US",
                "LOUISIANA",
                30.8703881,
                -92.007126,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "ME",
            ("US", "MAINE", 45.709097, -68.8590201, None, "US", "USA", "840"),
        ),
        (
            "MD",
            (
                "US",
                "MARYLAND",
                39.5162234,
                -76.9382069,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "MA",
            (
                "US",
                "MASSACHUSETTS",
                42.3788774,
                -72.032366,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "MI",
            (
                "US",
                "MICHIGAN",
                43.6211955,
                -84.6824346,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "MN",
            (
                "US",
                "MINNESOTA",
                45.9896587,
                -94.6113288,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "MS",
            (
                "US",
                "MISSISSIPPI",
                32.9715645,
                -89.7348497,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "MO",
            (
                "US",
                "MISSOURI",
                38.7604815,
                -92.5617875,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "MT",
            (
                "US",
                "MONTANA",
                47.3752671,
                -109.6387579,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "NE",
            (
                "US",
                "NEBRASKA",
                41.7370229,
                -99.5873816,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "NV",
            (
                "US",
                "NEVADA",
                39.5158825,
                -116.8537227,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "NH",
            (
                "US",
                "NEW HAMPSHIRE",
                43.4849133,
                -71.6553992,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "NJ",
            (
                "US",
                "NEW JERSEY",
                40.0757384,
                -74.4041622,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "NM",
            (
                "US",
                "NEW MEXICO",
                34.5708167,
                -105.993007,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "NY",
            (
                "US",
                "NEW YORK",
                40.7127281,
                -74.0060152,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "NC",
            (
                "US",
                "NORTH CAROLINA",
                35.6729639,
                -79.0392919,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "ND",
            (
                "US",
                "NORTH DAKOTA",
                47.6201461,
                -100.540737,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "OH",
            ("US", "OHIO", 40.2253569, -82.6881395, None, "US", "USA", "840"),
        ),
        (
            "OK",
            (
                "US",
                "OKLAHOMA",
                34.9550817,
                -97.2684063,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "OR",
            (
                "US",
                "OREGON",
                43.9792797,
                -120.737257,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "PA",
            (
                "US",
                "PENNSYLVANIA",
                40.9699889,
                -77.7278831,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "RI",
            (
                "US",
                "RHODE ISLAND",
                41.7962409,
                -71.5992372,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "SC",
            (
                "US",
                "SOUTH CAROLINA",
                33.6874388,
                -80.4363743,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "SD",
            (
                "US",
                "SOUTH DAKOTA",
                44.6471761,
                -100.348761,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "TN",
            (
                "US",
                "TENNESSEE",
                35.7730076,
                -86.2820081,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "TX",
            ("US", "TEXAS", 31.8160381, -99.5120986, None, "US", "USA", "840"),
        ),
        (
            "X1",
            (
                "US",
                "UNITED STATES",
                39.7837304,
                -100.4458825,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "UT",
            ("US", "UTAH", 39.4225192, -111.7143584, None, "US", "USA", "840"),
        ),
        (
            "VT",
            (
                "US",
                "VERMONT",
                44.5990718,
                -72.5002608,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "VA",
            (
                "US",
                "VIRGINIA",
                37.1232245,
                -78.4927721,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "WA",
            (
                "US",
                "WASHINGTON",
                47.2868352,
                -120.2126139,
                "WASHINGTON STATE, US",
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "WV",
            (
                "US",
                "WEST VIRGINIA",
                38.4758406,
                -80.8408415,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "WI",
            (
                "US",
                "WISCONSIN",
                44.4308975,
                -89.6884637,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "WY",
            (
                "US",
                "WYOMING",
                43.1700264,
                -107.5685348,
                None,
                "US",
                "USA",
                "840",
            ),
        ),
        (
            "A0",
            (
                "CANADA",
                "ALBERTA, CANADA",
                55.001251,
                -115.002136,
                None,
                "CA",
                "CAN",
                "124",
            ),
        ),
        (
            "A1",
            (
                "CANADA",
                "BRITISH COLUMBIA, CANADA",
                55.001251,
                -125.002441,
                None,
                "CA",
                "CAN",
                "124",
            ),
        ),
        (
            "A2",
            (
                "CANADA",
                "MANITOBA, CANADA",
                55.001251,
                -97.001038,
                None,
                "CA",
                "CAN",
                "124",
            ),
        ),
        (
            "A3",
            (
                "CANADA",
                "NEW BRUNSWICK, CANADA",
                46.500283,
                -66.750183,
                None,
                "CA",
                "CAN",
                "124",
            ),
        ),
        (
            "A4",
            (
                "CANADA",
                "NEWFOUNDLAND, CANADA",
                49.12120935,
                -56.69629621274099,
                None,
                "CA",
                "CAN",
                "124",
            ),
        ),
        (
            "A5",
            (
                "CANADA",
                "NOVA SCOTIA, CANADA",
                45.1960403,
                -63.1653789,
                None,
                "CA",
                "CAN",
                "124",
            ),
        ),
        (
            "A6",
            (
                "CANADA",
                "ONTARIO, CANADA",
                50.000678,
                -86.000977,
                None,
                "CA",
                "CAN",
                "124",
            ),
        ),
        (
            "A7",
            (
                "CANADA",
                "PRINCE EDWARD ISLAND, CANADA",
                46.503545349999996,
                -63.595517139914485,
                None,
                "CA",
                "CAN",
                "124",
            ),
        ),
        (
            "A8",
            (
                "CANADA",
                "QUEBEC, CANADA",
                52.4760892,
                -71.8258668,
                None,
                "CA",
                "CAN",
                "124",
            ),
        ),
        (
            "A9",
            (
                "CANADA",
                "SASKATCHEWAN, CANADA",
                55.5321257,
                -106.1412243,
                None,
                "CA",
                "CAN",
                "124",
            ),
        ),
        (
            "B0",
            (
                "CANADA",
                "YUKON, CANADA",
                63.000147,
                -136.002502,
                None,
                "CA",
                "CAN",
                "124",
            ),
        ),
        (
            "Z4",
            (
                "CANADA (Federal Level)",
                None,
                61.0666922,
                -107.9917071,
                None,
                "CA",
                "CAN",
                "124",
            ),
        ),
        (
            "B2",
            (
                "AFGHANISTAN",
                None,
                33.7680065,
                66.2385139,
                None,
                "AF",
                "AFG",
                "004",
            ),
        ),
        (
            "Y6",
            (
                "ÅLAND ISLANDS",
                None,
                60.1603621,
                20.08317860965865,
                None,
                "AX",
                "ALA",
                "248",
            ),
        ),
        (
            "B3",
            ("ALBANIA", None, 41.000028, 19.9999619, None, "AL", "ALB", "008"),
        ),
        (
            "B4",
            ("ALGERIA", None, 28.0000272, 2.9999825, None, "DZ", "DZA", "012"),
        ),
        (
            "B5",
            (
                "AMERICAN SAMOA",
                None,
                -14.289304,
                -170.692511,
                None,
                "AS",
                "ASM",
                "016",
            ),
        ),
        (
            "B6",
            ("ANDORRA", None, 42.5407167, 1.5732033, None, "AD", "AND", "020"),
        ),
        (
            "B7",
            (
                "ANGOLA",
                None,
                -11.8775768,
                17.5691241,
                None,
                "AO",
                "AGO",
                "024",
            ),
        ),
        (
            "1A",
            (
                "ANGUILLA",
                None,
                18.1954947,
                -63.0750234,
                None,
                "AI",
                "AIA",
                "660",
            ),
        ),
        (
            "B8",
            (
                "ANTARCTICA",
                None,
                -79.4063075,
                0.3149312,
                None,
                "AQ",
                "ATA",
                "010",
            ),
        ),
        (
            "B9",
            (
                "ANTIGUA AND BARBUDA",
                None,
                17.2234721,
                -61.9554608,
                None,
                "AG",
                "ATG",
                "028",
            ),
        ),
        (
            "C1",
            (
                "ARGENTINA",
                None,
                -34.9964963,
                -64.9672817,
                None,
                "AR",
                "ARG",
                "032",
            ),
        ),
        (
            "1B",
            (
                "ARMENIA",
                None,
                40.7696272,
                44.6736646,
                None,
                "AM",
                "ARM",
                "051",
            ),
        ),
        (
            "1C",
            ("ARUBA", None, 12.5013629, -69.9618475, None, "AW", "ABW", "533"),
        ),
        (
            "C3",
            (
                "AUSTRALIA",
                None,
                -24.7761086,
                134.755,
                None,
                "AU",
                "AUS",
                "036",
            ),
        ),
        (
            "C4",
            ("AUSTRIA", None, 47.2000338, 13.199959, None, "AT", "AUT", "040"),
        ),
        (
            "1D",
            (
                "AZERBAIJAN",
                None,
                40.3936294,
                47.7872508,
                None,
                "AZ",
                "AZE",
                "031",
            ),
        ),
        (
            "C5",
            (
                "BAHAMAS",
                None,
                24.7736546,
                -78.0000547,
                None,
                "BS",
                "BHS",
                "044",
            ),
        ),
        (
            "C6",
            (
                "BAHRAIN",
                None,
                26.1551249,
                50.5344606,
                None,
                "BH",
                "BHR",
                "048",
            ),
        ),
        (
            "C7",
            (
                "BANGLADESH",
                None,
                24.4768783,
                90.2932426,
                None,
                "BD",
                "BGD",
                "050",
            ),
        ),
        (
            "C8",
            (
                "BARBADOS",
                None,
                13.1500331,
                -59.5250305,
                None,
                "BB",
                "BRB",
                "052",
            ),
        ),
        (
            "1F",
            (
                "BELARUS",
                None,
                53.4250605,
                27.6971358,
                None,
                "BY",
                "BLR",
                "112",
            ),
        ),
        (
            "C9",
            ("BELGIUM", None, 50.6402809, 4.6667145, None, "BE", "BEL", "056"),
        ),
        (
            "D1",
            (
                "BELIZE",
                None,
                16.8259793,
                -88.7600927,
                None,
                "BZ",
                "BLZ",
                "084",
            ),
        ),
        (
            "G6",
            ("BENIN", None, 9.5293472, 2.2584408, None, "BJ", "BEN", "204"),
        ),
        (
            "D0",
            (
                "BERMUDA",
                None,
                32.3018217,
                -64.7603583,
                None,
                "BM",
                "BMU",
                "060",
            ),
        ),
        (
            "D2",
            ("BHUTAN", None, 27.549511, 90.5119273, None, "BT", "BTN", "064"),
        ),
        (
            "D3",
            (
                "BOLIVIA",
                None,
                -17.0568696,
                -64.9912286,
                None,
                "BO",
                "BOL",
                "068",
            ),
        ),
        (
            "1E",
            (
                "BOSNIA AND HERZEGOVINA",
                None,
                44.3053476,
                17.5961467,
                None,
                "BA",
                "BIH",
                "070",
            ),
        ),
        (
            "B1",
            (
                "BOTSWANA",
                None,
                -23.1681782,
                24.5928742,
                None,
                "BW",
                "BWA",
                "072",
            ),
        ),
        (
            "D4",
            (
                "BOUVET ISLAND",
                None,
                -54.4201305,
                3.3599732952297483,
                None,
                "BV",
                "BVT",
                "074",
            ),
        ),
        ("D5", ("BRAZIL", None, -10.3333333, -53.2, None, "BR", "BRA", "076")),
        (
            "D6",
            (
                "BRITISH INDIAN OCEAN TERRITORY",
                None,
                -6.4157192,
                72.1173961,
                None,
                "IO",
                "IOT",
                "086",
            ),
        ),
        (
            "D9",
            (
                "BRUNEI DARUSSALAM",
                None,
                4.4137155,
                114.5653908,
                None,
                "BN",
                "BRN",
                "096",
            ),
        ),
        (
            "E0",
            (
                "BULGARIA",
                None,
                42.6073975,
                25.4856617,
                None,
                "BG",
                "BGR",
                "100",
            ),
        ),
        (
            "X2",
            (
                "BURKINA FASO",
                None,
                12.0753083,
                -1.6880314,
                None,
                "BF",
                "BFA",
                "854",
            ),
        ),
        (
            "E2",
            (
                "BURUNDI",
                None,
                -3.3634357,
                29.8870575,
                None,
                "BI",
                "BDI",
                "108",
            ),
        ),
        (
            "E3",
            (
                "CAMBODIA",
                None,
                13.5066394,
                104.869423,
                None,
                "KH",
                "KHM",
                "116",
            ),
        ),
        (
            "E4",
            (
                "CAMEROON",
                None,
                4.6125522,
                13.1535811,
                None,
                "CM",
                "CMR",
                "120",
            ),
        ),
        (
            "E8",
            (
                "CAPE VERDE",
                None,
                16.0000552,
                -24.0083947,
                None,
                "CV",
                "CPV",
                "132",
            ),
        ),
        (
            "E9",
            (
                "CAYMAN ISLANDS",
                None,
                19.5417212,
                -80.5667132,
                None,
                "KY",
                "CYM",
                "136",
            ),
        ),
        (
            "F0",
            (
                "CENTRAL AFRICAN REPUBLIC",
                None,
                7.0323598,
                19.9981227,
                None,
                "CF",
                "CAF",
                "140",
            ),
        ),
        (
            "F2",
            ("CHAD", None, 15.6134137, 19.0156172, None, "TD", "TCD", "148"),
        ),
        (
            "F3",
            (
                "CHILE",
                None,
                -31.7613365,
                -71.3187697,
                None,
                "CL",
                "CHL",
                "152",
            ),
        ),
        (
            "F4",
            ("CHINA", None, 35.000074, 104.999927, None, "CN", "CHN", "156"),
        ),
        (
            "F6",
            (
                "CHRISTMAS ISLAND",
                None,
                -10.49124145,
                105.6173514897963,
                None,
                "CX",
                "CXR",
                "162",
            ),
        ),
        (
            "F7",
            (
                "COCOS (KEELING) ISLANDS",
                None,
                -12.0728315,
                96.8409375,
                None,
                "CC",
                "CCK",
                "166",
            ),
        ),
        (
            "F8",
            (
                "COLOMBIA",
                None,
                2.8894434,
                -73.783892,
                None,
                "CO",
                "COL",
                "170",
            ),
        ),
        (
            "F9",
            (
                "COMOROS",
                None,
                -12.2045176,
                44.2832964,
                None,
                "KM",
                "COM",
                "174",
            ),
        ),
        (
            "G0",
            ("CONGO", None, -0.7264327, 15.6419155, None, "CG", "COG", "178"),
        ),
        (
            "Y3",
            (
                "CONGO, THE DEMOCRATIC REPUBLIC OF THE",
                None,
                -2.9814344,
                23.8222636,
                "DEMOCRATIC REPUBLIC OF THE CONGO",
                "CD",
                "COD",
                "180",
            ),
        ),
        (
            "G1",
            (
                "COOK ISLANDS",
                None,
                -16.0492781,
                -160.3554851,
                None,
                "CK",
                "COK",
                "184",
            ),
        ),
        (
            "G2",
            (
                "COSTA RICA",
                None,
                10.2735633,
                -84.0739102,
                None,
                "CR",
                "CRI",
                "188",
            ),
        ),
        (
            "L7",
            (
                "COTE D'IVOIRE",
                None,
                7.9897371,
                -5.5679458,
                None,
                "CI",
                "CIV",
                "384",
            ),
        ),
        (
            "1M",
            (
                "CROATIA",
                None,
                45.5643442,
                17.0118954,
                None,
                "HR",
                "HRV",
                "191",
            ),
        ),
        (
            "G3",
            ("CUBA", None, 23.0131338, -80.8328748, None, "CU", "CUB", "192"),
        ),
        (
            "G4",
            ("CYPRUS", None, 34.9823018, 33.1451285, None, "CY", "CYP", "196"),
        ),
        (
            "2N",
            (
                "CZECH REPUBLIC",
                None,
                49.8167003,
                15.4749544,
                None,
                "CZ",
                "CZE",
                "203",
            ),
        ),
        (
            "G7",
            ("DENMARK", None, 55.670249, 10.3333283, None, "DK", "DNK", "208"),
        ),
        (
            "1G",
            (
                "DJIBOUTI",
                None,
                11.8145966,
                42.8453061,
                None,
                "DJ",
                "DJI",
                "262",
            ),
        ),
        (
            "G9",
            (
                "DOMINICA",
                None,
                19.0974031,
                -70.3028026,
                None,
                "DM",
                "DMA",
                "212",
            ),
        ),
        (
            "G8",
            (
                "DOMINICAN REPUBLIC",
                None,
                19.0974031,
                -70.3028026,
                None,
                "DO",
                "DOM",
                "214",
            ),
        ),
        (
            "H1",
            (
                "ECUADOR",
                None,
                -1.3397668,
                -79.3666965,
                None,
                "EC",
                "ECU",
                "218",
            ),
        ),
        (
            "H2",
            ("EGYPT", None, 26.2540493, 29.2675469, None, "EG", "EGY", "818"),
        ),
        (
            "H3",
            (
                "EL SALVADOR",
                None,
                13.8000382,
                -88.9140683,
                None,
                "SV",
                "SLV",
                "222",
            ),
        ),
        (
            "H4",
            (
                "EQUATORIAL GUINEA",
                None,
                1.613172,
                10.517037,
                None,
                "GQ",
                "GNQ",
                "226",
            ),
        ),
        (
            "1J",
            (
                "ERITREA",
                None,
                15.9500319,
                37.9999668,
                None,
                "ER",
                "ERI",
                "232",
            ),
        ),
        (
            "1H",
            (
                "ESTONIA",
                None,
                58.7523778,
                25.3319078,
                None,
                "EE",
                "EST",
                "233",
            ),
        ),
        (
            "H5",
            (
                "ETHIOPIA",
                None,
                10.2116702,
                38.6521203,
                None,
                "ET",
                "ETH",
                "231",
            ),
        ),
        (
            "H7",
            (
                "FALKLAND ISLANDS (MALVINAS)",
                None,
                -51.9666424,
                -59.5500387,
                None,
                "FK",
                "FLK",
                "238",
            ),
        ),
        (
            "H6",
            (
                "FAROE ISLANDS",
                None,
                62.0448724,
                -7.0322972,
                None,
                "FO",
                "FRO",
                "234",
            ),
        ),
        (
            "H8",
            ("FIJI", None, -18.1239696, 179.0122737, None, "FJ", "FJI", "242"),
        ),
        (
            "H9",
            (
                "FINLAND",
                None,
                63.2467777,
                25.9209164,
                None,
                "FI",
                "FIN",
                "246",
            ),
        ),
        (
            "I0",
            ("FRANCE", None, 46.603354, 1.8883335, None, "FR", "FRA", "250"),
        ),
        (
            "I3",
            (
                "FRENCH GUIANA",
                None,
                4.0039882,
                -52.999998,
                None,
                "GF",
                "GUF",
                "254",
            ),
        ),
        (
            "I4",
            (
                "FRENCH POLYNESIA",
                None,
                -16.03442485,
                -146.0490931059517,
                None,
                "PF",
                "PYF",
                "258",
            ),
        ),
        (
            "2C",
            (
                "FRENCH SOUTHERN TERRITORIES",
                None,
                -49.237441950000004,
                69.62275903679347,
                None,
                "TF",
                "ATF",
                "260",
            ),
        ),
        (
            "I5",
            ("GABON", None, -0.8999695, 11.6899699, None, "GA", "GAB", "266"),
        ),
        (
            "I6",
            ("GAMBIA", None, 13.470062, -15.4900464, None, "GM", "GMB", "270"),
        ),
        (
            "2Q",
            (
                "GEORGIA",
                None,
                41.6809707,
                44.0287382,
                None,
                "GE",
                "GEO",
                "268",
            ),
        ),
        (
            "2M",
            (
                "GERMANY",
                None,
                51.0834196,
                10.4234469,
                None,
                "DE",
                "DEU",
                "276",
            ),
        ),
        (
            "J0",
            ("GHANA", None, 8.0300284, -1.0800271, None, "GH", "GHA", "288"),
        ),
        (
            "J1",
            (
                "GIBRALTAR",
                None,
                36.140807,
                -5.3541295,
                None,
                "GI",
                "GIB",
                "292",
            ),
        ),
        (
            "J3",
            ("GREECE", None, 38.9953683, 21.9877132, None, "GR", "GRC", "300"),
        ),
        (
            "J4",
            (
                "GREENLAND",
                None,
                77.6192349,
                -42.8125967,
                None,
                "GL",
                "GRL",
                "304",
            ),
        ),
        (
            "J5",
            (
                "GRENADA",
                None,
                12.1360374,
                -61.6904045,
                None,
                "GD",
                "GRD",
                "308",
            ),
        ),
        (
            "J6",
            (
                "GUADELOUPE",
                None,
                16.2490067,
                -61.5650444,
                None,
                "GP",
                "GLP",
                "312",
            ),
        ),
        (
            "GU",
            (
                "GUAM",
                None,
                13.450125700000001,
                144.75755102972062,
                None,
                "GU",
                "GUM",
                "316",
            ),
        ),
        (
            "J8",
            (
                "GUATEMALA",
                None,
                15.6356088,
                -89.8988087,
                None,
                "GT",
                "GTM",
                "320",
            ),
        ),
        (
            "Y7",
            (
                "GUERNSEY",
                None,
                49.579520200000005,
                -2.5290434448309886,
                None,
                "GG",
                "GGY",
                "831",
            ),
        ),
        (
            "J9",
            (
                "GUINEA",
                None,
                10.7226226,
                -10.7083587,
                None,
                "GN",
                "GIN",
                "324",
            ),
        ),
        (
            "S0",
            (
                "GUINEA-BISSAU",
                None,
                12.100035,
                -14.9000214,
                None,
                "GW",
                "GNB",
                "624",
            ),
        ),
        (
            "K0",
            ("GUYANA", None, 4.8417097, -58.6416891, None, "GY", "GUY", "328"),
        ),
        (
            "K1",
            ("HAITI", None, 19.1399952, -72.3570972, None, "HT", "HTI", "332"),
        ),
        (
            "K4",
            (
                "HEARD ISLAND AND MCDONALD ISLANDS",
                None,
                -53.0166353,
                72.955751,
                None,
                "HM",
                "HMD",
                "334",
            ),
        ),
        (
            "X4",
            (
                "HOLY SEE (VATICAN CITY STATE)",
                None,
                41.9034912,
                12.4528349,
                "VATICAN CITY STATE",
                "VA",
                "VAT",
                "336",
            ),
        ),
        (
            "K2",
            (
                "HONDURAS",
                None,
                15.2572432,
                -86.0755145,
                None,
                "HN",
                "HND",
                "340",
            ),
        ),
        (
            "K3",
            (
                "HONG KONG",
                None,
                22.2793278,
                114.1628131,
                None,
                "HK",
                "HKG",
                "344",
            ),
        ),
        (
            "K5",
            (
                "HUNGARY",
                None,
                47.1817585,
                19.5060937,
                None,
                "HU",
                "HUN",
                "348",
            ),
        ),
        (
            "K6",
            (
                "ICELAND",
                None,
                64.9841821,
                -18.1059013,
                None,
                "IS",
                "ISL",
                "352",
            ),
        ),
        (
            "K7",
            ("INDIA", None, 22.3511148, 78.6677428, None, "IN", "IND", "356"),
        ),
        (
            "K8",
            (
                "INDONESIA",
                None,
                -2.4833826,
                117.8902853,
                None,
                "ID",
                "IDN",
                "360",
            ),
        ),
        (
            "K9",
            (
                "IRAN, ISLAMIC REPUBLIC OF",
                None,
                32.6475314,
                54.5643516,
                "ISLAMIC REPUBLIC OF IRAN",
                "IR",
                "IRN",
                "364",
            ),
        ),
        (
            "L0",
            ("IRAQ", None, 33.0955793, 44.1749775, None, "IQ", "IRQ", "368"),
        ),
        (
            "L2",
            ("IRELAND", None, 52.865196, -7.9794599, None, "IE", "IRL", "372"),
        ),
        (
            "Y8",
            (
                "ISLE OF MAN",
                None,
                54.2358167,
                -4.514598745698255,
                None,
                "IM",
                "IMN",
                "833",
            ),
        ),
        (
            "L3",
            ("ISRAEL", None, 31.5313113, 34.8667654, None, "IL", "ISR", "376"),
        ),
        (
            "L6",
            ("ITALY", None, 42.6384261, 12.674297, None, "IT", "ITA", "380"),
        ),
        (
            "L8",
            (
                "JAMAICA",
                None,
                18.1152958,
                -77.1598454610168,
                None,
                "JM",
                "JAM",
                "388",
            ),
        ),
        (
            "M0",
            ("JAPAN", None, 36.5748441, 139.2394179, None, "JP", "JPN", "392"),
        ),
        (
            "Y9",
            (
                "JERSEY",
                None,
                49.21230655,
                -2.1255999596428845,
                None,
                "JE",
                "JEY",
                "832",
            ),
        ),
        (
            "M2",
            ("JORDAN", None, 31.1667049, 36.941628, None, "JO", "JOR", "400"),
        ),
        (
            "1P",
            (
                "KAZAKSTAN",
                None,
                47.2286086,
                65.2093197,
                None,
                "KZ",
                "KAZ",
                "398",
            ),
        ),
        (
            "M3",
            ("KENYA", None, 1.4419683, 38.4313975, None, "KE", "KEN", "404"),
        ),
        (
            "J2",
            ("KIRIBATI", None, 0.306, 173.664834025, None, "KI", "KIR", "296"),
        ),
        (
            "M4",
            (
                "KOREA, DEMOCRATIC PEOPLE'S REPUBLIC OF",
                None,
                40.3736611,
                127.0870417,
                "NORTH KOREA",
                "KP",
                "PRK",
                "408",
            ),
        ),
        (
            "M5",
            (
                "KOREA, REPUBLIC OF",
                None,
                36.638392,
                127.6961188,
                "SOUTH KOREA",
                "KR",
                "KOR",
                "410",
            ),
        ),
        (
            "M6",
            ("KUWAIT", None, 29.2733964, 47.4979476, None, "KW", "KWT", "414"),
        ),
        (
            "1N",
            (
                "KYRGYZSTAN",
                None,
                41.5089324,
                74.724091,
                None,
                "KG",
                "KGZ",
                "417",
            ),
        ),
        (
            "M7",
            (
                "LAO PEOPLE'S DEMOCRATIC REPUBLIC",
                None,
                20.0171109,
                103.378253,
                None,
                "LA",
                "LAO",
                "418",
            ),
        ),
        (
            "1R",
            ("LATVIA", None, 56.8406494, 24.7537645, None, "LV", "LVA", "428"),
        ),
        (
            "M8",
            ("LEBANON", None, 33.8750629, 35.843409, None, "LB", "LBN", "422"),
        ),
        (
            "M9",
            (
                "LESOTHO",
                None,
                -29.6039267,
                28.3350193,
                None,
                "LS",
                "LSO",
                "426",
            ),
        ),
        (
            "N0",
            ("LIBERIA", None, 5.7499721, -9.3658524, None, "LR", "LBR", "430"),
        ),
        (
            "N1",
            (
                "LIBYAN ARAB JAMAHIRIYA",
                None,
                26.8234472,
                18.1236723,
                None,
                "LY",
                "LBY",
                "434",
            ),
        ),
        (
            "N2",
            (
                "LIECHTENSTEIN",
                None,
                47.1416307,
                9.5531527,
                None,
                "LI",
                "LIE",
                "438",
            ),
        ),
        (
            "1Q",
            (
                "LITHUANIA",
                None,
                55.3500003,
                23.7499997,
                None,
                "LT",
                "LTU",
                "440",
            ),
        ),
        (
            "N4",
            (
                "LUXEMBOURG",
                None,
                49.8158683,
                6.1296751,
                None,
                "LU",
                "LUX",
                "442",
            ),
        ),
        (
            "N5",
            ("MACAU", None, 22.1757605, 113.5514142, None, "MO", "MAC", "446"),
        ),
        (
            "1U",
            (
                "MACEDONIA, THE FORMER YUGOSLAV REPUBLIC OF",
                None,
                41.6171214,
                21.7168387,
                "MACEDONIA",
                "MK",
                "MKD",
                "807",
            ),
        ),
        (
            "N6",
            (
                "MADAGASCAR",
                None,
                -18.9249604,
                46.4416422,
                None,
                "MG",
                "MDG",
                "450",
            ),
        ),
        (
            "N7",
            (
                "MALAWI",
                None,
                -13.2687204,
                33.9301963,
                None,
                "MW",
                "MWI",
                "454",
            ),
        ),
        (
            "N8",
            (
                "MALAYSIA",
                None,
                4.5693754,
                102.2656823,
                None,
                "MY",
                "MYS",
                "458",
            ),
        ),
        (
            "N9",
            (
                "MALDIVES",
                None,
                4.7064352,
                73.3287853,
                None,
                "MV",
                "MDV",
                "462",
            ),
        ),
        (
            "O0",
            ("MALI", None, 16.3700359, -2.2900239, None, "ML", "MLI", "466"),
        ),
        (
            "O1",
            ("MALTA", None, 35.8885993, 14.4476911, None, "MT", "MLT", "470"),
        ),
        (
            "1T",
            (
                "MARSHALL ISLANDS",
                None,
                6.9518742,
                170.9985095,
                None,
                "MH",
                "MHL",
                "584",
            ),
        ),
        (
            "O2",
            (
                "MARTINIQUE",
                None,
                14.6113732,
                -60.9620777,
                None,
                "MQ",
                "MTQ",
                "474",
            ),
        ),
        (
            "O3",
            (
                "MAURITANIA",
                None,
                20.2540382,
                -9.2399263,
                None,
                "MR",
                "MRT",
                "478",
            ),
        ),
        (
            "O4",
            (
                "MAURITIUS",
                None,
                -20.2759451,
                57.5703566,
                None,
                "MU",
                "MUS",
                "480",
            ),
        ),
        (
            "2P",
            (
                "MAYOTTE",
                None,
                -12.823048,
                45.1520755,
                None,
                "YT",
                "MYT",
                "175",
            ),
        ),
        (
            "O5",
            (
                "MEXICO",
                None,
                19.4326296,
                -99.1331785,
                None,
                "MX",
                "MEX",
                "484",
            ),
        ),
        (
            "1K",
            (
                "MICRONESIA, FEDERATED STATES OF",
                None,
                8.6065,
                152.00846930625,
                "FEDERATED STATES OF MICRONESIA",
                "FM",
                "FSM",
                "583",
            ),
        ),
        (
            "1S",
            (
                "MOLDOVA, REPUBLIC OF",
                None,
                47.2879608,
                28.5670941,
                "MOLDOVA",
                "MD",
                "MDA",
                "498",
            ),
        ),
        (
            "O9",
            ("MONACO", None, 43.7323492, 7.4276832, None, "MC", "MCO", "492"),
        ),
        (
            "P0",
            (
                "MONGOLIA",
                None,
                46.8250388,
                103.8499736,
                None,
                "MN",
                "MNG",
                "496",
            ),
        ),
        (
            "Z5",
            (
                "MONTENEGRO",
                None,
                42.9868853,
                19.5180992,
                None,
                "ME",
                "MNE",
                "499",
            ),
        ),
        (
            "P1",
            (
                "MONTSERRAT",
                None,
                16.7417041,
                -62.1916844,
                None,
                "MS",
                "MSR",
                "500",
            ),
        ),
        (
            "P2",
            (
                "MOROCCO",
                None,
                31.1728205,
                -7.3362482,
                None,
                "MA",
                "MAR",
                "504",
            ),
        ),
        (
            "P3",
            (
                "MOZAMBIQUE",
                None,
                -19.302233,
                34.9144977,
                None,
                "MZ",
                "MOZ",
                "508",
            ),
        ),
        (
            "E1",
            (
                "MYANMAR",
                None,
                17.1750495,
                95.9999652,
                None,
                "MM",
                "MMR",
                "104",
            ),
        ),
        (
            "T6",
            (
                "NAMIBIA",
                None,
                -23.2335499,
                17.3231107,
                None,
                "NA",
                "NAM",
                "516",
            ),
        ),
        (
            "P5",
            ("NAURU", None, -0.5252306, 166.9324426, None, "NR", "NRU", "520"),
        ),
        (
            "P6",
            ("NEPAL", None, 28.1083929, 84.0917139, None, "NP", "NPL", "524"),
        ),
        (
            "P7",
            (
                "NETHERLANDS",
                None,
                52.5001698,
                5.7480821,
                None,
                "NL",
                "NLD",
                "528",
            ),
        ),
        (
            "P8",
            (
                "NETHERLANDS ANTILLES",
                None,
                12.1546009,
                -68.94047234929069,
                None,
                "AN",
                "ANT",
                "530",
            ),
        ),
        (
            "1W",
            (
                "NEW CALEDONIA",
                None,
                -20.454288599999998,
                164.55660583077983,
                None,
                "NC",
                "NCL",
                "540",
            ),
        ),
        (
            "Q2",
            (
                "NEW ZEALAND",
                None,
                -41.5000831,
                172.8344077,
                None,
                "NZ",
                "NZL",
                "554",
            ),
        ),
        (
            "Q3",
            (
                "NICARAGUA",
                None,
                12.6090157,
                -85.2936911,
                None,
                "NI",
                "NIC",
                "558",
            ),
        ),
        (
            "Q4",
            ("NIGER", None, 17.7356214, 9.3238432, None, "NE", "NER", "562"),
        ),
        (
            "Q5",
            ("NIGERIA", None, 9.6000359, 7.9999721, None, "NG", "NGA", "566"),
        ),
        (
            "Q6",
            (
                "NIUE",
                None,
                -19.0536414,
                -169.8613411,
                None,
                "NU",
                "NIU",
                "570",
            ),
        ),
        (
            "Q7",
            (
                "NORFOLK ISLAND",
                None,
                -29.0289575,
                167.9587289126371,
                None,
                "NF",
                "NFK",
                "574",
            ),
        ),
        (
            "1V",
            (
                "NORTHERN MARIANA ISLANDS",
                None,
                14.149020499999999,
                145.21345248318923,
                None,
                "MP",
                "MNP",
                "580",
            ),
        ),
        (
            "Q8",
            (
                "NORWAY",
                None,
                64.5731537,
                11.52803643954819,
                None,
                "NO",
                "NOR",
                "578",
            ),
        ),
        (
            "P4",
            ("OMAN", None, 21.0000287, 57.0036901, None, "OM", "OMN", "512"),
        ),
        (
            "R0",
            (
                "PAKISTAN",
                None,
                30.3308401,
                71.247499,
                None,
                "PK",
                "PAK",
                "586",
            ),
        ),
        (
            "1Y",
            ("PALAU", None, 6.097367, 133.313631, None, "PW", "PLW", "585"),
        ),
        (
            "1X",
            (
                "PALESTINIAN TERRITORY, OCCUPIED",
                None,
                31.94696655,
                35.27386547291496,
                "PALESTINE",
                "PS",
                "PSE",
                "275",
            ),
        ),
        (
            "R1",
            ("PANAMA", None, 8.559559, -81.1308434, None, "PA", "PAN", "591"),
        ),
        (
            "R2",
            (
                "PAPUA NEW GUINEA",
                None,
                -5.6816069,
                144.2489081,
                None,
                "PG",
                "PNG",
                "598",
            ),
        ),
        (
            "R4",
            (
                "PARAGUAY",
                None,
                -23.3165935,
                -58.1693445,
                None,
                "PY",
                "PRY",
                "600",
            ),
        ),
        (
            "R5",
            ("PERU", None, -6.8699697, -75.0458515, None, "PE", "PER", "604"),
        ),
        (
            "R6",
            (
                "PHILIPPINES",
                None,
                12.7503486,
                122.7312101,
                None,
                "PH",
                "PHL",
                "608",
            ),
        ),
        (
            "R8",
            (
                "PITCAIRN",
                None,
                -25.0657719,
                -130.1017823,
                None,
                "PN",
                "PCN",
                "612",
            ),
        ),
        (
            "R9",
            ("POLAND", None, 52.215933, 19.134422, None, "PL", "POL", "616"),
        ),
        (
            "S1",
            (
                "PORTUGAL",
                None,
                40.0332629,
                -7.8896263,
                None,
                "PT",
                "PRT",
                "620",
            ),
        ),
        (
            "PR",
            (
                "PUERTO RICO",
                None,
                18.2214149,
                -66.41328179513847,
                None,
                "PR",
                "PRI",
                "630",
            ),
        ),
        (
            "S3",
            ("QATAR", None, 25.3336984, 51.2295295, None, "QA", "QAT", "634"),
        ),
        (
            "S4",
            (
                "REUNION",
                None,
                -21.1309332,
                55.5265771,
                None,
                "RE",
                "REU",
                "638",
            ),
        ),
        (
            "S5",
            (
                "ROMANIA",
                None,
                45.9852129,
                24.6859225,
                None,
                "RO",
                "ROU",
                "642",
            ),
        ),
        (
            "1Z",
            (
                "RUSSIAN FEDERATION",
                None,
                64.6863136,
                97.7453061,
                None,
                "RU",
                "RUS",
                "643",
            ),
        ),
        (
            "S6",
            ("RWANDA", None, -1.9646631, 30.0644358, None, "RW", "RWA", "646"),
        ),
        (
            "Z0",
            (
                "SAINT BARTHELEMY",
                None,
                17.9036287,
                -62.811568843006896,
                None,
                "BL",
                "BLM",
                "652",
            ),
        ),
        (
            "U8",
            (
                "SAINT HELENA",
                None,
                -15.9656162,
                -5.702147693859718,
                None,
                "SH",
                "SHN",
                "654",
            ),
        ),
        (
            "U7",
            (
                "SAINT KITTS AND NEVIS",
                None,
                17.250512,
                -62.6725973,
                None,
                "KN",
                "KNA",
                "659",
            ),
        ),
        (
            "U9",
            (
                "SAINT LUCIA",
                None,
                13.8250489,
                -60.975036,
                None,
                "LC",
                "LCA",
                "662",
            ),
        ),
        (
            "Z1",
            (
                "SAINT MARTIN",
                None,
                48.5683066,
                6.7539988,
                None,
                "MF",
                "MAF",
                "663",
            ),
        ),
        (
            "V0",
            (
                "SAINT PIERRE AND MIQUELON",
                None,
                46.783246899999995,
                -56.195158907484085,
                None,
                "PM",
                "SPM",
                "666",
            ),
        ),
        (
            "V1",
            (
                "SAINT VINCENT AND THE GRENADINES",
                None,
                12.90447,
                -61.2765569,
                None,
                "VC",
                "VCT",
                "670",
            ),
        ),
        (
            "Y0",
            (
                "SAMOA",
                None,
                -13.7693895,
                -172.1200508,
                None,
                "WS",
                "WSM",
                "882",
            ),
        ),
        (
            "S8",
            (
                "SAN MARINO",
                None,
                43.9458623,
                12.458306,
                None,
                "SM",
                "SMR",
                "674",
            ),
        ),
        (
            "S9",
            (
                "SAO TOME AND PRINCIPE",
                None,
                0.8875498,
                6.9648718,
                None,
                "ST",
                "STP",
                "678",
            ),
        ),
        (
            "T0",
            (
                "SAUDI ARABIA",
                None,
                25.6242618,
                42.3528328,
                None,
                "SA",
                "SAU",
                "682",
            ),
        ),
        (
            "T1",
            (
                "SENEGAL",
                None,
                14.4750607,
                -14.4529612,
                None,
                "SN",
                "SEN",
                "686",
            ),
        ),
        (
            "Z2",
            (
                "SERBIA",
                None,
                44.024322850000004,
                21.07657433209902,
                None,
                "RS",
                "SRB",
                "688",
            ),
        ),
        (
            "T2",
            (
                "SEYCHELLES",
                None,
                -4.6574977,
                55.4540146,
                None,
                "SC",
                "SYC",
                "690",
            ),
        ),
        (
            "T8",
            (
                "SIERRA LEONE",
                None,
                8.6400349,
                -11.8400269,
                None,
                "SL",
                "SLE",
                "694",
            ),
        ),
        (
            "U0",
            (
                "SINGAPORE",
                None,
                1.357107,
                103.8194992,
                None,
                "SG",
                "SGP",
                "702",
            ),
        ),
        (
            "2B",
            (
                "SLOVAKIA",
                None,
                48.7411522,
                19.4528646,
                None,
                "SK",
                "SVK",
                "703",
            ),
        ),
        (
            "2A",
            (
                "SLOVENIA",
                None,
                45.8133113,
                14.4808369,
                None,
                "SI",
                "SVN",
                "705",
            ),
        ),
        (
            "D7",
            (
                "SOLOMON ISLANDS",
                None,
                -9.7354344,
                162.8288542,
                None,
                "SB",
                "SLB",
                "090",
            ),
        ),
        (
            "U1",
            ("SOMALIA", None, 8.3676771, 49.083416, None, "SO", "SOM", "706"),
        ),
        (
            "T3",
            (
                "SOUTH AFRICA",
                None,
                -28.8166236,
                24.991639,
                None,
                "ZA",
                "ZAF",
                "710",
            ),
        ),
        (
            "1L",
            (
                "SOUTH GEORGIA AND THE SOUTH SANDWICH ISLANDS",
                None,
                -54.8432857,
                -35.8090698,
                None,
                "GS",
                "SGS",
                "239",
            ),
        ),
        (
            "U3",
            ("SPAIN", None, 39.3262345, -4.8380649, None, "ES", "ESP", "724"),
        ),
        (
            "F1",
            (
                "SRI LANKA",
                None,
                7.5554942,
                80.7137847,
                None,
                "LK",
                "LKA",
                "144",
            ),
        ),
        (
            "V2",
            ("SUDAN", None, 14.5844444, 29.4917691, None, "SD", "SDN", "729"),
        ),
        (
            "V3",
            (
                "SURINAME",
                None,
                4.1413025,
                -56.0771187,
                None,
                "SR",
                "SUR",
                "740",
            ),
        ),
        (
            "L9",
            (
                "SVALBARD AND JAN MAYEN",
                None,
                78.51240255,
                16.60558240163109,
                None,
                "SJ",
                "SJM",
                "744",
            ),
        ),
        (
            "V6",
            (
                "SWAZILAND",
                None,
                -26.5624806,
                31.3991317,
                None,
                "SZ",
                "SWZ",
                "748",
            ),
        ),
        (
            "V7",
            ("SWEDEN", None, 59.6749712, 14.5208584, None, "SE", "SWE", "752"),
        ),
        (
            "V8",
            (
                "SWITZERLAND",
                None,
                46.7985624,
                8.2319736,
                None,
                "CH",
                "CHE",
                "756",
            ),
        ),
        (
            "V9",
            (
                "SYRIAN ARAB REPUBLIC",
                None,
                34.6401861,
                39.0494106,
                None,
                "SY",
                "SYR",
                "760",
            ),
        ),
        (
            "F5",
            (
                "TAIWAN, PROVINCE OF CHINA",
                None,
                23.9739374,
                120.9820179,
                "TAIWAN",
                "TW",
                "TWN",
                "158",
            ),
        ),
        (
            "2D",
            (
                "TAJIKISTAN",
                None,
                38.6281733,
                70.8156541,
                None,
                "TJ",
                "TJK",
                "762",
            ),
        ),
        (
            "W0",
            (
                "TANZANIA, UNITED REPUBLIC OF",
                None,
                -6.5247123,
                35.7878438,
                "UNITED REPUBLIC OF TANZANIA",
                "TZ",
                "TZA",
                "834",
            ),
        ),
        (
            "W1",
            (
                "THAILAND",
                None,
                14.8971921,
                100.83273,
                None,
                "TH",
                "THA",
                "764",
            ),
        ),
        (
            "Z3",
            (
                "TIMOR-LESTE",
                None,
                -8.5151979,
                125.8375756,
                None,
                "TL",
                "TLS",
                "626",
            ),
        ),
        ("W2", ("TOGO", None, 8.7800265, 1.0199765, None, "TG", "TGO", "768")),
        (
            "W3",
            (
                "TOKELAU",
                None,
                -9.1676396,
                -171.8196878,
                None,
                "TK",
                "TKL",
                "772",
            ),
        ),
        (
            "W4",
            (
                "TONGA",
                None,
                -19.9160819,
                -175.2026424,
                None,
                "TO",
                "TON",
                "776",
            ),
        ),
        (
            "W5",
            (
                "TRINIDAD AND TOBAGO",
                None,
                10.8677845,
                -60.9821067,
                None,
                "TT",
                "TTO",
                "780",
            ),
        ),
        (
            "W6",
            ("TUNISIA", None, 33.8439408, 9.400138, None, "TN", "TUN", "788"),
        ),
        (
            "W8",
            ("TURKEY", None, 38.9597594, 34.9249653, None, "TR", "TUR", "792"),
        ),
        (
            "2E",
            (
                "TURKMENISTAN",
                None,
                39.3763807,
                59.3924609,
                None,
                "TM",
                "TKM",
                "795",
            ),
        ),
        (
            "W7",
            (
                "TURKS AND CAICOS ISLANDS",
                None,
                21.7214683,
                -71.6201783,
                None,
                "TC",
                "TCA",
                "796",
            ),
        ),
        (
            "2G",
            ("TUVALU", None, -7.768959, 178.1167698, None, "TV", "TUV", "798"),
        ),
        (
            "W9",
            ("UGANDA", None, 1.5333554, 32.2166578, None, "UG", "UGA", "800"),
        ),
        (
            "2H",
            (
                "UKRAINE",
                None,
                49.4871968,
                31.2718321,
                None,
                "UA",
                "UKR",
                "804",
            ),
        ),
        (
            "C0",
            (
                "UNITED ARAB EMIRATES",
                None,
                24.0002488,
                53.9994829,
                None,
                "AE",
                "ARE",
                "784",
            ),
        ),
        (
            "X0",
            (
                "UNITED KINGDOM",
                None,
                54.7023545,
                -3.2765753,
                None,
                "GB",
                "GBR",
                "826",
            ),
        ),
        (
            "2J",
            (
                "UNITED STATES MINOR OUTLYING ISLANDS",
                None,
                38.893661249999994,
                -76.98788325388196,
                "DISTRICT OF COLUMBIA, US",
                "UM",
                "UMI",
                "581",
            ),
        ),
        (
            "X3",
            (
                "URUGUAY",
                None,
                -32.8755548,
                -56.0201525,
                None,
                "UY",
                "URY",
                "858",
            ),
        ),
        (
            "2K",
            (
                "UZBEKISTAN",
                None,
                41.32373,
                63.9528098,
                None,
                "UZ",
                "UZB",
                "860",
            ),
        ),
        (
            "2L",
            (
                "VANUATU",
                None,
                -16.5255069,
                168.1069154,
                None,
                "VU",
                "VUT",
                "548",
            ),
        ),
        (
            "X5",
            (
                "VENEZUELA",
                None,
                8.0018709,
                -66.1109318,
                None,
                "VE",
                "VEN",
                "862",
            ),
        ),
        (
            "Q1",
            (
                "VIET NAM",
                None,
                13.2904027,
                108.4265113,
                None,
                "VN",
                "VNM",
                "704",
            ),
        ),
        (
            "D8",
            (
                "VIRGIN ISLANDS, BRITISH",
                None,
                18.4024395,
                -64.5661642,
                "BRITISH VIRGIN ISLANDS",
                "VG",
                "VGB",
                "092",
            ),
        ),
        (
            "VI",
            (
                "VIRGIN ISLANDS, U.S.",
                None,
                17.789187,
                -64.7080574,
                "U.S. VIRGIN ISLANDS",
                "VI",
                "VIR",
                "850",
            ),
        ),
        (
            "X8",
            (
                "WALLIS AND FUTUNA",
                None,
                -14.30190495,
                -178.08997342208266,
                None,
                "WF",
                "WLF",
                "876",
            ),
        ),
        (
            "U5",
            (
                "WESTERN SAHARA",
                None,
                24.1797324,
                -13.7667848,
                None,
                "EH",
                "ESH",
                "732",
            ),
        ),
        (
            "T7",
            ("YEMEN", None, 16.3471243, 47.8915271, None, "YE", "YEM", "887"),
        ),
        (
            "Y4",
            (
                "ZAMBIA",
                None,
                -14.5186239,
                27.5599164,
                None,
                "ZM",
                "ZMB",
                "894",
            ),
        ),
        (
            "Y5",
            (
                "ZIMBABWE",
                None,
                -18.4554963,
                29.7468414,
                None,
                "ZW",
                "ZWE",
                "716",
            ),
        ),
        (
            "L4",
            ("ISRAEL", None, 30.8124247, 34.8594762, None, "IL", "ISR", "376"),
        ),
        (
            "L5",
            ("ISRAEL", None, 30.8124247, 34.8594762, None, "IL", "ISR", "376"),
        ),
        (
            "XX",
            (
                "UNKNOWN",
                None,
                38.893661249999994,
                -76.98788325388196,
                "DISTRICT OF COLUMBIA, US",
                "00",
                "000",
                "000",
            ),
        ),
    ]
)


def _make_alpha_2() -> dict[str, str]:
    result = {v[5]: k for k, v in STATE_CODES.items() if v[5]}
    # fix US and CA codes:
    result["US"] = "X1"
    result["CA"] = "Z4"
    return result


state_codes_by_alpha_2: dict[str, str] = _make_alpha_2()


CACHED_CORE_TABLES: list[str] = [
    getattr(ESEF.EsefEntity, "__tablename__", ""),
    getattr(ESEF.EsefEntityOtherName, "__tablename__", ""),
    getattr(ESEF.EsefFiling, "__tablename__", ""),
    getattr(ESEF.EsefFilingLang, "__tablename__", ""),
    getattr(ESEF.EsefFilingError, "__tablename__", ""),
    getattr(ESEF.EsefInferredFilingLanguage, "__tablename__", ""),
    getattr(SEC.SecFiler, "__tablename__", ""),
    getattr(SEC.SecFormerNames, "__tablename__", ""),
]

import datetime
import isodate
from _typeshed import Incomplete
from arelle import PythonUtil as PythonUtil
from arelle.ModelObject import ModelObject as ModelObject
from typing import Any, Optional

XmlUtil: Incomplete

def qname(value: Union[ModelObject, str, QName, Any, None], name: Union[str, ModelObject, None] = ..., noPrefixIsNoNamespace: bool = ..., castException: Union[Exception, None] = ..., prefixException: Union[Exception, None] = ...) -> Union[QName, None]: ...
def qnameHref(href): ...
def qnameNsLocalName(namespaceURI, localName): ...
def qnameClarkName(clarkname): ...
def qnameEltPfxName(element, prefixedName, prefixException: Incomplete | None = ...): ...

class QName:
    prefix: Incomplete
    namespaceURI: Incomplete
    localName: Incomplete
    qnameValueHash: Incomplete
    def __init__(self, prefix: Optional[str], namespaceURI: Optional[str], localName: Optional[str]) -> None: ...
    def __hash__(self): ...
    @property
    def clarkNotation(self) -> Optional[str]: ...
    @property
    def expandedName(self) -> str: ...
    def __eq__(self, other): ...
    def __lt__(self, other): ...
    def __bool__(self): ...

def anyURI(value): ...

class AnyURI(str):
    def __new__(cls, value): ...

datetimePattern: Incomplete
timePattern: Incomplete
durationPattern: Incomplete
DATE: int
DATETIME: int
DATEUNION: int

def tzinfo(tz): ...
def tzinfoStr(dt): ...
def dateTime(value, time: Incomplete | None = ..., addOneDay: Incomplete | None = ..., type: Incomplete | None = ..., castException: Incomplete | None = ...): ...
def lastDayOfMonth(year, month): ...

class DateTime(datetime.datetime):
    def __new__(cls, y, m, d, hr: int = ..., min: int = ..., sec: int = ..., microsec: int = ..., tzinfo: Incomplete | None = ..., dateOnly: Incomplete | None = ..., addOneDay: Incomplete | None = ...): ...
    def __copy__(self): ...
    def addYearMonthDuration(self, other, sign): ...
    def __add__(self, other): ...
    def __sub__(self, other): ...

def dateUnionEqual(dateUnion1, dateUnion2, instantEndDate: bool = ...): ...
def dateunionDate(datetimeValue, subtractOneDay: bool = ...): ...
def yearMonthDuration(value): ...

class YearMonthDuration:
    years: Incomplete
    months: Incomplete
    def __init__(self, years, months) -> None: ...

def dayTimeDuration(value): ...

class DayTimeDuration(datetime.timedelta):
    def __new__(cls, days, hours, minutes, seconds): ...
    def dayHrsMinsSecs(self): ...

def yearMonthDayTimeDuration(value, value2: Incomplete | None = ...): ...

class YearMonthDayTimeDuration:
    years: Incomplete
    months: Incomplete
    days: Incomplete
    hours: Incomplete
    minutes: Incomplete
    seconds: Incomplete
    def __init__(self, years, months, days, hours, minutes, seconds) -> None: ...

def time(value, castException: Incomplete | None = ...): ...

class Time(datetime.time):
    def __new__(cls, hour: int = ..., minute: int = ..., second: int = ..., microsecond: int = ..., tzinfo: Incomplete | None = ...): ...

class gYearMonth:
    year: Incomplete
    month: Incomplete
    def __init__(self, year, month) -> None: ...
    def __eq__(self, other): ...
    def __ne__(self, other): ...
    def __lt__(self, other): ...
    def __le__(self, other): ...
    def __gt__(self, other): ...
    def __ge__(self, other): ...
    def __bool__(self): ...

class gMonthDay:
    month: Incomplete
    day: Incomplete
    def __init__(self, month, day) -> None: ...
    def __eq__(self, other): ...
    def __ne__(self, other): ...
    def __lt__(self, other): ...
    def __le__(self, other): ...
    def __gt__(self, other): ...
    def __ge__(self, other): ...
    def __bool__(self): ...

class gYear:
    year: Incomplete
    def __init__(self, year) -> None: ...
    def __eq__(self, other): ...
    def __ne__(self, other): ...
    def __lt__(self, other): ...
    def __le__(self, other): ...
    def __gt__(self, other): ...
    def __ge__(self, other): ...
    def __bool__(self): ...

class gMonth:
    month: Incomplete
    def __init__(self, month) -> None: ...
    def __eq__(self, other): ...
    def __ne__(self, other): ...
    def __lt__(self, other): ...
    def __le__(self, other): ...
    def __gt__(self, other): ...
    def __ge__(self, other): ...
    def __bool__(self): ...

class gDay:
    day: Incomplete
    def __init__(self, day) -> None: ...
    def __eq__(self, other): ...
    def __ne__(self, other): ...
    def __lt__(self, other): ...
    def __le__(self, other): ...
    def __gt__(self, other): ...
    def __ge__(self, other): ...
    def __bool__(self): ...

isoDurationPattern: Incomplete

def isoDuration(value): ...

DAYSPERMONTH: Incomplete

class IsoDuration(isodate.Duration):
    years: Incomplete
    months: Incomplete
    tdelta: Incomplete
    sourceValue: Incomplete
    avgdays: Incomplete
    def __init__(self, days: int = ..., seconds: int = ..., microseconds: int = ..., milliseconds: int = ..., minutes: int = ..., hours: int = ..., weeks: int = ..., months: int = ..., years: int = ..., negate: bool = ..., sourceValue: Incomplete | None = ...) -> None: ...
    def __hash__(self): ...
    def __eq__(self, other): ...
    def __ne__(self, other): ...
    def __lt__(self, other): ...
    def __le__(self, other): ...
    def __gt__(self, other): ...
    def __ge__(self, other): ...
    def viewText(self, labelrole: Incomplete | None = ..., lang: Incomplete | None = ...): ...

class InvalidValue(str):
    def __new__(cls, value): ...

INVALIDixVALUE: Incomplete

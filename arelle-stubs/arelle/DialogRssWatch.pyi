from _typeshed import Incomplete
from arelle import XmlUtil as XmlUtil
from arelle.CntlrWinTooltip import ToolTip as ToolTip
from arelle.ModelValue import dateTime as dateTime
from arelle.PluginManager import pluginClassMethods as pluginClassMethods
from arelle.UiUtil import checkbox as checkbox, gridCell as gridCell, gridCombobox as gridCombobox, label as label
from arelle.UrlUtil import isValidAbsolute as isValidAbsolute
from tkinter import Toplevel

def getOptions(mainWin) -> None: ...

rssFeeds: Incomplete
emailPattern: Incomplete

class DialogRssWatch(Toplevel):
    mainWin: Incomplete
    parent: Incomplete
    options: Incomplete
    accepted: bool
    cellFeed: Incomplete
    cellMatchText: Incomplete
    cellFormulaFile: Incomplete
    cellLogFile: Incomplete
    cellEmailAddress: Incomplete
    cellLatestPubDate: Incomplete
    checkboxes: Incomplete
    def __init__(self, mainWin, options) -> None: ...
    def chooseFormulaFile(self) -> None: ...
    def chooseLogFile(self) -> None: ...
    def setupSmtp(self) -> None: ...
    def clearPubDate(self) -> None: ...
    def checkEntries(self): ...
    def setOptions(self) -> None: ...
    def ok(self, event: Incomplete | None = ...) -> None: ...
    def close(self, event: Incomplete | None = ...) -> None: ...

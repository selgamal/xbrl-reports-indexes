from _typeshed import Incomplete
from arelle import XmlUtil as XmlUtil
from arelle.ModelObject import ModelObject as ModelObject

newRssWatchOptions: Incomplete

class ModelRssItem(ModelObject):
    status: Incomplete
    results: Incomplete
    assertions: Incomplete
    edgr: Incomplete
    edgrDescription: Incomplete
    edgrFile: Incomplete
    edgrInlineXBRL: Incomplete
    edgrSequence: Incomplete
    edgrType: Incomplete
    edgrUrl: Incomplete
    def init(self, modelDocument) -> None: ...
    @property
    def cikNumber(self): ...
    @property
    def accessionNumber(self): ...
    @property
    def fileNumber(self): ...
    @property
    def companyName(self): ...
    @property
    def formType(self): ...
    @property
    def pubDate(self): ...
    @property
    def filingDate(self): ...
    @property
    def period(self): ...
    @property
    def assignedSic(self): ...
    @property
    def acceptanceDatetime(self): ...
    @property
    def fiscalYearEnd(self): ...
    @property
    def htmlUrl(self): ...
    @property
    def url(self): ...
    @property
    def enclosureUrl(self): ...
    @property
    def zippedUrl(self): ...
    @property
    def htmURLs(self): ...
    @property
    def primaryDocumentURL(self): ...
    assertionUnsuccessful: bool
    def setResults(self, modelXbrl) -> None: ...
    @property
    def propertyView(self): ...

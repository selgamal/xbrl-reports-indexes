from _typeshed import Incomplete
from arelle import XmlUtil as XmlUtil
from arelle.ModelDocument import ModelDocument as ModelDocument, Type as Type

class ModelRssObject(ModelDocument):
    rssItems: Incomplete
    def __init__(self, modelXbrl, type=..., uri: Incomplete | None = ..., filepath: Incomplete | None = ..., xmlDocument: Incomplete | None = ...) -> None: ...
    xmlRootElement: Incomplete
    def rssFeedDiscover(self, rootElement) -> None: ...

from _typeshed import Incomplete
from arelle import XbrlConst as XbrlConst, XmlUtil as XmlUtil
from arelle.ModelDtsObject import ModelAll as ModelAll, ModelAny as ModelAny, ModelAnyAttribute as ModelAnyAttribute, ModelAttribute as ModelAttribute, ModelAttributeGroup as ModelAttributeGroup, ModelChoice as ModelChoice, ModelConcept as ModelConcept, ModelEnumeration as ModelEnumeration, ModelGroupDefinition as ModelGroupDefinition, ModelLink as ModelLink, ModelLocator as ModelLocator, ModelResource as ModelResource, ModelRoleType as ModelRoleType, ModelSequence as ModelSequence, ModelType as ModelType
from arelle.ModelObject import ModelObject as ModelObject
from arelle.ModelRssItem import ModelRssItem as ModelRssItem
from arelle.ModelTestcaseObject import ModelTestcaseVariation as ModelTestcaseVariation
from arelle.ModelValue import qnameNsLocalName as qnameNsLocalName
from arelle.ModelVersObject import ModelAction as ModelAction, ModelAssignment as ModelAssignment, ModelConceptDetailsChange as ModelConceptDetailsChange, ModelConceptUseChange as ModelConceptUseChange, ModelNamespaceRename as ModelNamespaceRename, ModelRelationshipSet as ModelRelationshipSet, ModelRelationshipSetChange as ModelRelationshipSetChange, ModelRelationships as ModelRelationships, ModelRoleChange as ModelRoleChange, ModelVersObject as ModelVersObject
from lxml import etree

elementSubstitutionModelClass: Incomplete
ModelDocument: Incomplete
ModelFact: Incomplete

def parser(modelXbrl, baseUrl, target: Incomplete | None = ...): ...
def setParserElementClassLookup(parser, modelXbrl, baseUrl: Incomplete | None = ...): ...

SCHEMA: int
LINKBASE: int
VERSIONINGREPORT: int
RSSFEED: int

class KnownNamespacesModelObjectClassLookup(etree.CustomElementClassLookup):
    modelXbrl: Incomplete
    type: Incomplete
    def __init__(self, modelXbrl, fallback: Incomplete | None = ...) -> None: ...
    def lookup(self, node_type, document, ns, ln): ...

class DiscoveringClassLookup(etree.PythonElementClassLookup):
    modelXbrl: Incomplete
    streamingOrSkipDTS: Incomplete
    baseUrl: Incomplete
    discoveryAttempts: Incomplete
    def __init__(self, modelXbrl, baseUrl, fallback: Incomplete | None = ...) -> None: ...
    def lookup(self, document, proxyElement): ...

"""Database model for ESEF XBRL filings index https://filings.xbrl.org/

Data is extracted from https://filings.xbrl.org/index.json and lei api
"""
from __future__ import annotations

from urllib.parse import quote as urlquote
from urllib.parse import urljoin

from lxml import etree
from sqlalchemy import Column
from sqlalchemy import false
from sqlalchemy import true
from sqlalchemy.orm import relationship
from sqlalchemy.types import BOOLEAN
from xbrlreportsindexes.model import types_mapping
from xbrlreportsindexes.model.base_model import Base
from xbrlreportsindexes.model.base_model import CreatedUpdatedAtColMixin
from xbrlreportsindexes.model.base_model import Location


Location.__mapper__.add_property(
    "esef_filers",
    relationship(
        "EsefEntity",
        primaryjoin="Location.code==foreign(EsefEntity.location_code)",
        back_populates="location",
    ),
)


class EsefFiling(Base, CreatedUpdatedAtColMixin):
    """Stores filings"""

    __table_args__ = {"comment": "esef_index"}

    filing_id = Column(
        types_mapping.Bigint_type, primary_key=True, autoincrement=True
    )
    filing_key = Column(types_mapping.Text_type, nullable=False)
    filing_root = Column(types_mapping.Text_type, nullable=False)
    filing_number = Column(types_mapping.Integer_type, nullable=False)
    entity_lei = Column(types_mapping.Text_type)
    country = Column(types_mapping.Text_type, nullable=False)
    filing_system = Column(types_mapping.Text_type, nullable=False)
    filing_type = Column(
        types_mapping.Text_type, nullable=False, default="AFR"
    )
    date_added = Column(types_mapping.Date_type, nullable=False)
    report_date = Column(types_mapping.Date_type, nullable=False)
    xbrl_json_instance = Column(types_mapping.Text_type, nullable=True)
    report_package = Column(types_mapping.Text_type, nullable=True)
    report_document = Column(types_mapping.Text_type, nullable=True)
    viewer_document = Column(types_mapping.Text_type, nullable=True)
    is_loadable = Column(BOOLEAN(), nullable=False, default=true())
    load_error = Column(types_mapping.Text_type, nullable=True)
    is_amended_hint = Column(BOOLEAN(), nullable=True, default=false())
    other_langs_hint = Column(BOOLEAN(), nullable=True, default=false())
    esef_filer: EsefEntity = relationship(
        "EsefEntity",
        primaryjoin="EsefEntity.entity_lei==" "foreign(EsefFiling.entity_lei)",
        back_populates="esef_filings",
    )
    errors: list[EsefFilingError] = relationship(
        "EsefFilingError",
        primaryjoin="EsefFiling.filing_id=="
        "foreign(EsefFilingError.filing_id)",
    )
    langs: list[EsefFilingLang] = relationship(
        "EsefFilingLang",
        primaryjoin="EsefFiling.filing_id=="
        "foreign(EsefFilingLang.filing_id)",
    )
    inferred_langs: list[EsefInferredFilingLanguage] = relationship(
        "EsefInferredFilingLanguage",
        primaryjoin="EsefFiling.filing_id"
        "==foreign(EsefInferredFilingLanguage.filing_id)",
    )

    def _get_full_link(self, file: str) -> str | None:
        """Adds base url"""
        if file is None:
            file = ""

        base_url = "https://filings.xbrl.org/"
        filing_key = str(self.filing_key)
        if not self.filing_key.endswith("/"):
            filing_key += "/"
        filing_base_url = urljoin(base_url, filing_key)
        escaped_file = urlquote(file)
        return urljoin(filing_base_url, escaped_file)

    @property
    def report_package_link(self) -> str | None:
        """Gets report package link"""
        link = None
        if self.report_package is not None:
            link = self._get_full_link(str(self.report_package))
        return link

    @property
    def report_document_link(self) -> str | None:
        """Gets report document link"""
        return self._get_full_link(str(self.report_document))

    @property
    def viewer_document_link(self) -> str | None:
        """Gets ix viewer link"""
        link = None
        if self.viewer_document is not None:
            link = self._get_full_link(str(self.viewer_document))
        return link

    @property
    def xbrl_json_instance_link(self) -> str | None:
        """Gets json xbrl document link"""
        link = None
        if self.xbrl_json_instance is not None:
            link = self._get_full_link(str(self.xbrl_json_instance))
        return link

    @property
    def filing_link(self) -> str | None:
        """Gets link filing that lists all files"""
        return self._get_full_link("")

    def to_xml(
        self, parent: etree._Element, database_name: str
    ) -> etree._Element:
        """Returns item element similar to item element in rss feed"""
        time_format_tz = "%a, %d %b %Y %H:%M:%S %Z"
        time_format = "%a, %d %b %Y %H:%M:%S"
        # item elements
        item = etree.SubElement(parent, "item")
        item_title_elements = {
            "title": self.entity_lei,
            "link": self.filing_link,
            "guid": self.report_package_link,
            "description": f"{self.entity_lei} {self.filing_system} "
            f"({self.report_date})",
            "pubDate": self.date_added.strftime(time_format_tz)
            if getattr(self.date_added, "tzinfo", False)
            else self.date_added.strftime(time_format),
            "filing_id": str(self.filing_id),
            "database": database_name,
            "duplicate": "",
        }
        for tag, value in item_title_elements.items():
            etree.SubElement(item, tag).text = value
        if isinstance(self.report_package_link, str):
            etree.SubElement(
                item,
                "enclosure",
                url=self.report_package_link,
                length="unknown",
                type="application/zip",
            )

        edgar_ns = "https://www.sec.gov/Archives/edgar"
        edgar_nsmap = {"edgar": edgar_ns}
        xbrl_filing_element = etree.SubElement(
            item,
            etree.QName(
                "https://www.sec.gov/Archives/edgar", tag="xbrlFiling"
            ),
            nsmap=edgar_nsmap,
        )
        xbrl_filing_children = {
            "companyName": self.esef_filer.lei_legal_name,
            "formType": self.filing_type,
            "filingDate": self.date_added.strftime("%m/%d/%Y")
            if self.date_added
            else None,
            "cikNumber": self.entity_lei,
            "accessionNumber": self.filing_key,
            "fileNumber": self.filing_key,
            "acceptanceDatetime": self.date_added.strftime("%Y%m%d%H%M%S"),
            "assistantDirector": None,
            "assignedSic": None,
            "fiscalYearEnd": str(self.report_date),
        }
        for tag, value in xbrl_filing_children.items():
            etree.SubElement(
                xbrl_filing_element,
                etree.QName(edgar_ns, tag),
                nsmap=edgar_nsmap,
            ).text = value

        xbrl_files = etree.SubElement(
            xbrl_filing_element,
            etree.QName(edgar_ns, "xbrlFiles"),
            nsmap=edgar_nsmap,
        )

        for file in [
            dict(
                file=str(self.report_document),
                url=str(self.report_document_link),
                description="instance.INS",
                inline_xbrl="false",
                seq="1",
            ),
            dict(
                file=str(self.xbrl_json_instance),
                url=str(self.xbrl_json_instance_link),
                description="instance.json",
                inline_xbrl="false",
                seq="2",
            ),
            dict(
                file=str(self.viewer_document),
                url=str(self.viewer_document_link),
                description="instance.viewer",
                inline_xbrl="true",
                seq="3",
            ),
            dict(
                file=str(self.report_package),
                url=str(self.report_package_link),
                description="instance.package",
                inline_xbrl="false",
                seq="4",
            ),
        ]:
            if file.get("file", False):
                file_attributes = {
                    str(etree.QName(edgar_ns, "sequence")): file.get(
                        "seq", ""
                    ),
                    str(etree.QName(edgar_ns, "file")): file.get("file", ""),
                    str(etree.QName(edgar_ns, "type")): file.get(
                        "description", ""
                    ),
                    str(etree.QName(edgar_ns, "size")): "unknown",
                    str(etree.QName(edgar_ns, "description")): file.get(
                        "description", ""
                    ),
                    str(etree.QName(edgar_ns, "url")): file.get("url", ""),
                }

                if file.get("inline_xbrl", False):
                    file_attributes[
                        str(etree.QName(edgar_ns, "inlineXBRL"))
                    ] = "true"
                etree.SubElement(
                    xbrl_files,
                    etree.QName(edgar_ns, "xbrlFile"),
                    attrib=file_attributes,
                    nsmap=edgar_nsmap,
                )
        return item


class EsefEntity(Base, CreatedUpdatedAtColMixin):
    """ESEF entity information based on lei"""

    __table_args__ = {"comment": "esef_index"}
    entity_lei = Column(types_mapping.Text_type, primary_key=True)
    location_code = Column(types_mapping.Text_type, nullable=True)
    lei_legal_name = Column(types_mapping.Text_type, nullable=True)
    lei_legal_address_lines = Column(types_mapping.Text_type, nullable=True)
    lei_legal_address_city = Column(types_mapping.Text_type, nullable=True)
    lei_legal_address_country = Column(types_mapping.Text_type, nullable=True)
    lei_legal_address_postal_code = Column(
        types_mapping.Text_type, nullable=True
    )
    lei_hq_address_lines = Column(types_mapping.Text_type, nullable=True)
    lei_hq_address_city = Column(types_mapping.Text_type, nullable=True)
    lei_hq_address_country = Column(types_mapping.Text_type, nullable=True)
    lei_hq_address_postal_code = Column(types_mapping.Text_type, nullable=True)
    lei_category = Column(types_mapping.Text_type, nullable=True)
    lei_isin = Column(types_mapping.Text_type, nullable=True)
    industry = Column(types_mapping.Bigint_type, nullable=True)
    esef_filings: list[EsefFiling] = relationship(
        "EsefFiling",
        primaryjoin="EsefEntity.entity_lei==foreign(EsefFiling.entity_lei)",
        back_populates="esef_filer",
    )
    location: Location = relationship(
        "Location",
        primaryjoin="Location.code==foreign(EsefEntity.location_code)",
        back_populates="esef_filers",
    )
    other_names: list[EsefEntityOtherName] = relationship(
        "EsefEntityOtherName",
        primaryjoin="EsefEntity.entity_lei"
        "==foreign(EsefEntityOtherName.entity_lei)",
        back_populates="esef_entity",
    )


class EsefEntityOtherName(Base, CreatedUpdatedAtColMixin):
    """Other or previous entity names extracted from lei information"""

    __table_args__ = {"comment": "esef_index"}
    entity_lei = Column(types_mapping.Text_type, primary_key=True)
    other_name = Column(types_mapping.Text_type, primary_key=True)
    other_name_type = Column(types_mapping.Text_type, primary_key=True)
    esef_entity: EsefEntity = relationship(
        "EsefEntity",
        primaryjoin="EsefEntity.entity_lei"
        "==foreign(EsefEntityOtherName.entity_lei)",
        back_populates="other_names",
    )


class EsefFilingLang(Base, CreatedUpdatedAtColMixin):
    """Filing language as presented in https://filings.xbrl.org/"""

    __table_args__ = {"comment": "esef_index"}
    filing_id = Column(types_mapping.Integer_type, primary_key=True)
    lang = Column(
        types_mapping.Text_type,
        nullable=False,
        primary_key=True,
        default="unknown",
    )
    lang_name = Column(types_mapping.Text_type)


class EsefInferredFilingLanguage(Base, CreatedUpdatedAtColMixin):
    """Filing language inferred from json instance included with the filing"""

    __table_args__ = {"comment": "esef_index"}
    filing_id = Column(types_mapping.Bigint_type, primary_key=True)
    lang = Column(
        types_mapping.Text_type,
        nullable=False,
        primary_key=True,
        default="unknown",
    )
    lang_name = Column(types_mapping.Text_type)
    facts_in_lang = Column(types_mapping.Integer_type)
    facts_in_report = Column(types_mapping.Integer_type)


class EsefFilingError(Base):
    """Filing validation errors included in https://filings.xbrl.org/"""

    __table_args__ = {"comment": "esef_index"}
    error_id = Column(types_mapping.Bigint_type, primary_key=True)
    filing_id = Column(types_mapping.Bigint_type, nullable=False)
    severity = Column(types_mapping.Text_type, nullable=False)
    code = Column(types_mapping.Text_type, nullable=True)
    message = Column(types_mapping.Text_type, nullable=True)

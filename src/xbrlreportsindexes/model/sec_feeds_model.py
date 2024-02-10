"""mapping for sec xbrl filings rss feeds"""
from __future__ import annotations

from typing import Any
from urllib.parse import urljoin

from lxml import etree
from lxml import html
from lxml.etree import QName
from sqlalchemy import and_
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import join
from sqlalchemy import select
from sqlalchemy.orm import aliased
from sqlalchemy.orm import backref
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import Query
from sqlalchemy.orm import relationship
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session
from sqlalchemy.sql import table
from sqlalchemy.sql import TableClause
from sqlalchemy.types import BOOLEAN
from xbrlreportsindexes.model import create_view
from sqlalchemy.types import (
    BigInteger,
    DateTime,
    Date,
    Integer,
    Numeric,
    SmallInteger,
    String,
)
from xbrlreportsindexes.model.base_model import Base
from xbrlreportsindexes.model.base_model import CreatedUpdatedAtColMixin
from xbrlreportsindexes.model.base_model import FeedIdColMixin
from xbrlreportsindexes.model.base_model import FilingIdColMixin
from xbrlreportsindexes.model.base_model import Location
from xbrlreportsindexes.model.base_model import meta


try:
    from arelle.Cntlr import Cntlr
except ModuleNotFoundError as exc:
    raise Exception("Please add path to arelle to python path") from exc


LAST_MODIFIED_DATE_COL = "last_modified_date"

Location.__mapper__.add_property(
    "filers",
    relationship(
        "SecFiler",
        primaryjoin="Location.code==foreign(SecFiler.location_code)",
        back_populates="location",
    ),
)


class SecFeed(Base, CreatedUpdatedAtColMixin, FeedIdColMixin):
    """Monthly feeds"""

    __table_args__ = {"comment": "sec_rss"}
    # columns
    feed_month = Column(Date(), nullable=False)
    title = Column(String(), nullable=True)
    link = Column(String(), nullable=True)
    feed_link = Column(String(), nullable=False)
    description = Column(String(), nullable=True)
    language = Column(String(), nullable=True)
    pub_date = Column(DateTime(timezone=True), nullable=True)
    last_build_date = Column(DateTime(timezone=True), nullable=True)
    included_filings_count = Column(BigInteger(), nullable=True)
    included_files_count = Column(BigInteger(), nullable=True)
    last_modified_date = Column(
        LAST_MODIFIED_DATE_COL,
        DateTime(),
        nullable=True,
        index=True,
    )
    # relations
    filings: Mapped[list["SecFiling"]] = relationship("SecFiling", back_populates="feed")


class SecFiling(
    Base, CreatedUpdatedAtColMixin, FeedIdColMixin, FilingIdColMixin
):
    """SEC xbrl filing information"""

    __table_args__ = {"comment": "sec_rss"}
    # columns
    filing_link = Column(String(), nullable=True)
    filing_title = Column(String(), nullable=True)
    filing_description = Column(String(), nullable=True)
    primary_document_url = Column(String(), nullable=True)
    entry_point = Column(String(), nullable=True)
    enclosure_url = Column(String(), nullable=True)
    enclosure_size = Column(BigInteger(), nullable=True)
    pub_date = Column(DateTime(), nullable=True, index=True)
    company_name = Column(String(), nullable=True)
    form_type = Column(String(), nullable=True, index=True)
    inline_xbrl = Column(Integer(), nullable=True)
    filing_date = Column(
        DateTime(), nullable=True, index=True
    )
    cik_number = Column(String(), nullable=True)
    accession_number = Column(
        String(), nullable=True, index=True
    )
    file_number = Column(String(), nullable=True)
    acceptance_datetime = Column(DateTime(), nullable=True)
    period = Column(Date(), nullable=True)
    assigned_sic = Column(
        Integer(), nullable=True, index=True
    )
    assistant_director = Column(String(), nullable=True)
    fiscal_year_end = Column(String(), nullable=True)
    fiscal_year_end_month = Column(Integer(), nullable=True)
    fiscal_year_end_day = Column(Integer(), nullable=True)
    duplicate = Column(
        Integer(), nullable=True, index=True, default=0
    )
    # relations
    feed: Mapped[list["SecFeed"]] = relationship("SecFeed", back_populates="filings")
    files: Mapped[list["SecFile"]] = relationship("SecFile", back_populates="filing")
    filer: Mapped["SecFiler"] = relationship(
        "SecFiler",
        foreign_keys=[cik_number],
        primaryjoin="SecFiling.cik_number==SecFiler.cik_number",
        back_populates="filings",
    )
    industry: Mapped[list["SecIndustry"]] = relationship(
        "SecIndustry",
        primaryjoin=(
            "and_(foreign(SecFiling.assigned_sic)==SecIndustry.industry_code, "
            "SecIndustry.industry_classification=='SEC')"
        ),
        back_populates="filings",
    )

    def to_xml(
        self, parent: etree._Element, database_name: str
    ) -> etree._Element:
        """Returns item element similar to item element in rss feed"""
        time_format_tz = "%a, %d %b %Y %H:%M:%S %Z"
        time_format = "%a, %d %b %Y %H:%M:%S"
        # item elements
        item = etree.SubElement(parent, "item")
        item_title_elements = {
            "title": self.filing_title,
            "link": self.filing_link,
            "guid": self.enclosure_url if self.enclosure_url else "",
            "description": self.filing_description,
            "pubDate": self.pub_date.strftime(time_format_tz)
            if getattr(self.pub_date, "tzinfo", False)
            else self.pub_date.strftime(time_format),
            "filing_id": str(self.filing_id),
            "database": database_name,
            "duplicate": "true" if self.duplicate else "false",
        }
        for tag, value in item_title_elements.items():
            etree.SubElement(item, tag).text = value
        etree.SubElement(
            item,
            "enclosure",
            url=self.enclosure_url
            if isinstance(self.enclosure_url, str)
            else "",
            length=str(self.enclosure_size),
            type="application/zip",
        )

        edgar_ns = "https://www.sec.gov/Archives/edgar"
        edgar_nsmap = {"edgar": edgar_ns}
        xbrl_filing_element = etree.SubElement(
            item,
            QName("https://www.sec.gov/Archives/edgar", tag="xbrlFiling"),
            nsmap=edgar_nsmap,
        )
        xbrl_filing_children = {
            "companyName": self.company_name,
            "formType": self.form_type,
            "filingDate": self.filing_date.date().strftime("%m/%d/%Y")
            if self.filing_date
            else None,
            "cikNumber": self.cik_number,
            "accessionNumber": self.accession_number,
            "fileNumber": self.file_number,
            "acceptanceDatetime": self.acceptance_datetime.strftime(
                "%Y%m%d%H%M%S"
            ),
            "assistantDirector": self.assistant_director,
            "assignedSic": str(self.assigned_sic),
            "fiscalYearEnd": self.fiscal_year_end,
        }
        for tag, value in xbrl_filing_children.items():
            etree.SubElement(
                xbrl_filing_element, QName(edgar_ns, tag), nsmap=edgar_nsmap
            ).text = value

        xbrl_files = xbrl_filing_element.find("edgar:xbrlFiles", edgar_nsmap)
        xbrl_files = etree.SubElement(
            xbrl_filing_element,
            QName(edgar_ns, "xbrlFiles"),
            nsmap=edgar_nsmap,
        )
        for file_obj in self.files:
            file_obj.to_xml(xbrl_files)
        return item

    @property
    def inline_xbrl_viewer_link(self) -> str | None:
        """Link to inline xbrl viewer"""
        link = None
        if self.inline_xbrl:
            link = self.primary_document_url.replace(
                "www.sec.gov/", "www.sec.gov/ix?doc="
            )
        return link

    @property
    def interactive_document_link(self) -> str | None:
        """Link to interactive document"""
        return (
            f"https://www.sec.gov/cgi-bin/viewer?action=view&"
            f'cik={self.cik_number.lstrip("0")}&'
            f"accession_number={self.accession_number}&xbrl_type=v"
        )

    def get_extracted_document_link(self, cntlr: Cntlr) -> str | None:
        """Returns link to extracted document for inline
        xbrl filings"""
        guess_first = True
        extracted_doc: str
        if not self.inline_xbrl:
            return None  # No extracted doc

        if guess_first:
            # guess extracted doc link
            extracted_doc = (
                self.primary_document_url.rpartition(".")[0] + "_htm.xml"
            )
            # test
            test = cntlr.webCache.opener.open(extracted_doc)
            if test.code == 200:
                return extracted_doc
        # otherwise find the href of extracted doc
        if isinstance(self.filing_link, str):
            index_page = cntlr.webCache.opener.open(
                self.filing_link, timeout=5
            )
            tree = html.parse(index_page)
            extracted_path = tree.xpath(
                './/table[contains(@summary, "Data Files")]'
                '//*[contains(text(), "EXTRACTED")]'
                "/ancestor::tr/td[3]//@href"
            )[0]
            if extracted_path:
                extracted_doc = urljoin(self.filing_link, extracted_path)

        return extracted_doc


class SecFile(
    Base, CreatedUpdatedAtColMixin, FeedIdColMixin, FilingIdColMixin
):
    """Filing files"""

    __table_args__ = {"comment": "sec_rss"}
    # column
    file_id = Column(
        BigInteger(),
        primary_key=True,
        nullable=False,
        autoincrement=False,
    )
    accession_number = Column(String(), nullable=True)
    sequence = Column(Integer(), nullable=True)
    file = Column(String(), nullable=True)
    type = Column(String(), nullable=True)
    size = Column(BigInteger(), nullable=True)
    description = Column(String(), nullable=True)
    inline_xbrl = Column(Integer(), nullable=True)
    url = Column(String(), nullable=True)
    type_tag = Column(String(), nullable=True, index=True)
    duplicate = Column(Integer(), nullable=True, index=True)
    # relations
    filing: Mapped["SecFiling"] = relationship("SecFiling", back_populates="files")

    def to_xml(self, parent: etree._Element) -> etree._Element:
        """Returns file element similar to SEC rss feed file element"""
        edgar_ns = "https://www.sec.gov/Archives/edgar"
        edgar_nsmap = {"edgar": edgar_ns}
        xbrl_file = etree.SubElement(
            parent,
            QName(edgar_ns, "xbrlFile"),
            nsmap=edgar_nsmap,
        )
        xbrl_file.attrib[str(QName(edgar_ns, "sequence"))] = str(self.sequence)
        xbrl_file.attrib[str(QName(edgar_ns, "file"))] = str(self.file)
        xbrl_file.attrib[str(QName(edgar_ns, "type"))] = str(self.type)
        xbrl_file.attrib[str(QName(edgar_ns, "size"))] = str(self.size)
        xbrl_file.attrib[str(QName(edgar_ns, "description"))] = str(
            self.description
        )
        xbrl_file.attrib[str(QName(edgar_ns, "url"))] = str(self.url)
        if self.inline_xbrl:
            xbrl_file.attrib[str(QName(edgar_ns, "inlineXBRL"))] = "true"

        return xbrl_file


class SecFiler(Base, CreatedUpdatedAtColMixin):
    """Sec filer information"""

    __table_args__ = {"comment": "sec_rss"}
    # columns
    cik_number = Column(
        String(),
        nullable=False,
        primary_key=True,
        autoincrement=False,
        index=True,
    )
    industry_code = Column(
        Integer(), nullable=True, index=True
    )
    industry_description = Column(String(), nullable=True)
    state_of_incorporation = Column(String(), nullable=True)
    mailing_state = Column(String(), nullable=True)
    mailing_city = Column(String(), nullable=True)
    mailing_zip = Column(String(), nullable=True)
    conformed_name = Column(String(), nullable=True)
    business_city = Column(String(), nullable=True)
    business_state = Column(String(), nullable=True)
    business_zip = Column(String(), nullable=True)
    country = Column(String(), nullable=True)
    location_code = Column(String(), nullable=False)
    # relations
    filings: Mapped[list["SecFiling"]] = relationship(
        "SecFiling",
        primaryjoin="SecFiler.cik_number==foreign(SecFiling.cik_number)",
        back_populates="filer",
    )

    former_names: Mapped[list["SecFormerNames"]] = relationship(
        "SecFormerNames", back_populates="filer"
    )

    ticker_symbols: Mapped[list["SecCikTickerMapping"]] = relationship(
        "SecCikTickerMapping",
        primaryjoin="foreign(SecCikTickerMapping.cik_number)"
        "==SecFiler.cik_number",
        back_populates="filer",
    )

    location: Mapped["Location"] = relationship(
        "Location",
        primaryjoin="Location.code==foreign(SecFiler.location_code)",
        back_populates="filers",
    )


class SecFormerNames(Base, CreatedUpdatedAtColMixin):
    """Sec filers' former names"""

    __table_args__ = {"comment": "sec_rss"}
    # columns
    cik_number: Mapped[str] = mapped_column(
        String(),
        ForeignKey("sec_filer.cik_number"),
        autoincrement=False,
        nullable=False,
        primary_key=True,
    )
    name = Column(String(), nullable=False, primary_key=True)
    date_changed = Column(Date(), nullable=False)
    # relationships
    filer: Mapped["SecFiler"] = relationship("SecFiler", back_populates="former_names")


class SecCikTickerMapping(Base, CreatedUpdatedAtColMixin):
    """SEC cik to ticker mapping"""

    __table_args__ = {"comment": "sec_rss"}
    # columns
    cik_number = Column(
        String(), primary_key=True, nullable=False
    )
    ticker_symbol = Column(
        String(), primary_key=True, nullable=False
    )
    exchange = Column(
        String(), primary_key=False, nullable=True
    )
    company_name = Column(
        String(), primary_key=True, nullable=False
    )
    # relations
    filer: Mapped["SecFiler"] = relationship(
        "SecFiler",
        primaryjoin="foreign(SecCikTickerMapping.cik_number)"
        "==SecFiler.cik_number",
        back_populates="ticker_symbols",
    )


class SpCompaniesCiks(Base, CreatedUpdatedAtColMixin):
    """Listing of S&P 500 companies and cik numbers"""

    __table_args__ = {"comment": "sec_rss"}
    cik_number = Column(
        String(), nullable=False, primary_key=True
    )
    as_of_date = Column(Date(), nullable=True)
    is_sp100 = Column(BOOLEAN, nullable=True)
    ticker_symbol = Column(String(), nullable=True)
    date_first_added = Column(String(), nullable=True)


class SecIndustry(Base):
    """Standard industry classifications"""

    __table_args__ = {"comment": "sec_rss"}
    # columns
    industry_id = Column(
        BigInteger(),
        nullable=False,
        primary_key=True,
        autoincrement=False,
    )
    industry_classification = Column(
        String(), nullable=True
    )
    industry_code = Column(BigInteger(), nullable=True)
    industry_description = Column(
        String(), nullable=True
    )
    depth = Column(Integer(), nullable=True)
    parent_id = Column(
        BigInteger(), nullable=True
    )  # ForeignKey('industry.industry_id')
    # relations
    children: Mapped[list["SecIndustry"]] = relationship(
        "SecIndustry",
        primaryjoin="SecIndustry.industry_id==foreign(SecIndustry.parent_id)",
        backref=backref("parent", remote_side=[industry_id]),
    )
    filings: Mapped[list["SecFiling"]] = relationship(
        "SecFiling",
        primaryjoin=(
            "and_(SecIndustry.industry_code==foreign(SecFiling.assigned_sic),"
            "SecIndustry.industry_classification=='SEC')"
        ),
        back_populates="industry",
    )

    def all_industry_tree_filings(self, session: Session) -> Query[Any]:
        """returns a query object for Filing that can be filtered"""
        # session = object_session(self)
        industry_2 = aliased(SecIndustry)
        top_ = session.query(industry_2)
        top = top_.filter(industry_2.industry_id == self.industry_id).cte(
            name="sub_industry", recursive=True
        )
        bottom = session.query(industry_2)
        bottom = bottom.join(top, industry_2.parent_id == top.c.industry_id)
        recursive_q = top.union(bottom)
        all_filings = session.query(SecFiling).join(
            recursive_q,
            and_(
                SecFiling.assigned_sic == recursive_q.c.industry_code,
                recursive_q.c.industry_classification == "SEC",
            ),
        )
        return all_filings


class SecIndustryLevel(Base):
    """Standard industry classifications"""

    __table_args__ = {"comment": "sec_rss"}
    industry_level_id = Column(
        BigInteger(),
        nullable=False,
        primary_key=True,
        autoincrement=False,
    )
    industry_classification = Column(
        String(), nullable=True
    )
    ancestor_id = Column(BigInteger(), nullable=True)
    ancestor_code = Column(Integer(), nullable=True)
    ancestor_depth = Column(Integer(), nullable=True)
    descendant_id = Column(BigInteger(), nullable=True)
    descendant_code = Column(Integer(), nullable=True)
    descendant_depth = Column(Integer(), nullable=True)


class SecIndustryStructure(Base):
    """Standard industry classifications"""

    __table_args__ = {"comment": "sec_rss"}
    industry_structure_id = Column(
        BigInteger(),
        nullable=False,
        primary_key=True,
        autoincrement=False,
    )
    industry_classification = Column(
        String(), nullable=False
    )
    depth = Column(Integer(), nullable=False)
    level_name = Column(String(), nullable=True)


# create views
v_count_filing_by_feed: table = create_view.view(
    "v_count_filing_by_feed",
    meta,
    select(
        SecFeed.feed_id.label("feed_id"),
        SecFeed.feed_month.label("feed_month"),
        SecFeed.feed_link.label("feed_link"),
        SecFeed.last_modified_date.label("last_modified_date"),
        func.count(SecFiling.filing_id).label("count_filing"),
    )
    .select_from(join(SecFeed, SecFiling))
    .group_by(
        SecFeed.feed_id,
        SecFeed.feed_month,
        SecFeed.feed_link,
        SecFeed.last_modified_date,
    )
    .order_by(SecFeed.feed_id),
)

assert v_count_filing_by_feed.primary_key == [v_count_filing_by_feed.c.feed_id]


class ViewCountFilingByFeed(Base):
    """Count filings by feeds"""

    __table_args__ = {"comment": "sec_rss"}
    __table__: TableClause = v_count_filing_by_feed


# gets unmarked duplicates
v_duplicate_filing_cte = (
    select(
        SecFiling.accession_number.label("accession_number"),
        func.min(SecFiling.filing_id).label("filing_id"),
        func.count(SecFiling.filing_id).label("count_filing_ids"),
    )
    .select_from(SecFiling)
    .where(SecFiling.duplicate == 0)
    .group_by(SecFiling.accession_number)
).cte("x")

v_duplicate_filing = create_view.view(
    "v_duplicate_filing",
    meta,
    select(SecFiling.filing_id)
    .select_from(
        join(
            v_duplicate_filing_cte,
            SecFiling,
            onclause=v_duplicate_filing_cte.c.filing_id == SecFiling.filing_id,
        )
    )
    .where(v_duplicate_filing_cte.c.count_filing_ids > 1),
)

assert v_duplicate_filing.primary_key == [v_duplicate_filing.c.filing_id]


class ViewDuplicateFiling(Base):
    """Lists non tagged duplicate filings"""

    __table_args__ = {"comment": "sec_rss"}
    __table__ = v_duplicate_filing


v_filing_summary = create_view.view(
    "v_filing_summary",
    meta,
    select(
        SecFeed.feed_id.label("feed_id"),
        SecFiling.form_type.label("form_type"),
        SecFiling.assigned_sic.label("assigned_sic"),
        SecFiling.inline_xbrl.label("inline_xbrl"),
        func.count(SecFiling.filing_id).label("count_filing"),
    )
    .select_from(join(SecFeed, SecFiling))
    .where(SecFiling.duplicate == 0)
    .group_by(
        SecFeed.feed_id,
        SecFiling.form_type,
        SecFiling.assigned_sic,
        SecFiling.inline_xbrl,
    )
    .order_by(SecFeed.feed_id),
)

assert v_filing_summary.primary_key == [v_filing_summary.c.feed_id]


class ViewFilingSummary(Base):
    """Lists summary counts of filings"""

    __table_args__ = {"comment": "sec_rss"}
    __table__ = v_filing_summary

# type: ignore
"""Gather test data"""
# run => PYTHONPATH=$(pwd)
# python xbrl_reports_index/mock_test_data/gather_test_mocks.py
from __future__ import annotations

import json
from pathlib import Path

from lxml import etree


try:
    from arelle.Cntlr import Cntlr
except ModuleNotFoundError as exc:
    raise Exception("Please add path to arelle to python path") from exc

mock_data_dir = Path(__file__)

cntlr = Cntlr()

ciks = set()
sec_mocks = mock_data_dir.joinpath("sec")
for x in sec_mocks.joinpath("monthly").iterdir():
    if x.parts[-1].startswith("xbrlrss-"):
        cik_xpath = './/*[local-name()="cikNumber"]/text()'
        for cik in etree.parse(x).getroot().xpath(cik_xpath):
            ciks.add(cik)
            filer_url = (
                f"https://www.sec.gov/cgi-bin/browse-edgar?CIK="
                f"{cik}&action=getcompany&output=atom"
            )
            filer_data = cntlr.webCache.opener.open(filer_url)
            file = sec_mocks.parent.joinpath("ciks", cik)
            with open(file, "wb") as fh:
                fh.write(filer_data.read())

lei = set()
esef_mocks = mock_data_dir.joinpath("esef")
for x in esef_mocks.joinpath("index").iterdir():
    with open(x, "r", encoding="utf-8") as xh:
        for k in json.load(xh):
            lei.add(k)

url = (
    f"https://api.gleif.org/api/v1/lei-records?"
    f"page[size]=100&filter[lei]={','.join(lei)}"
)
resp = cntlr.webCache.opener.open(url)
with open(esef_mocks.joinpath("lei"), "wb") as j:
    j.write(resp.read())

for _lei in lei:
    url_isins = f"https://api.gleif.org/api/v1/lei-records/{_lei}/isins"
    resp = cntlr.webCache.opener.open(url_isins)
    with open(esef_mocks.joinpath("isin", _lei), "wb") as j:
        j.write(resp.read())

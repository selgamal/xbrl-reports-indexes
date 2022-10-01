[![test](https://github.com/selgamal/xbrl-reports-indexes/actions/workflows/test.yaml/badge.svg)](https://github.com/selgamal/xbrl-reports-indexes/actions/workflows/test.yaml) [![publish-pypi](https://github.com/selgamal/xbrl-reports-indexes/actions/workflows/publish.yaml/badge.svg)](https://github.com/selgamal/xbrl-reports-indexes/actions/workflows/publish.yaml)

# XBRL Reports Indexes (xri)
`xri` creates and updates a database for indexes of ESEF filings and SEC XBRL filings and filers (only filings metadata NOT filings contents). SEC XBRL filings index is based on data extracted from [SEC Monthly RSS Feeds](https://www.sec.gov/Archives/edgar/monthly/) for XBRL filings, ESEF XBRL filings index is based on data extracted from [filings.xbrl.org](https://filings.xbrl.org/).

This package is useful for easily collecting and organizing up to date information about XBRL filings available on SEC, including latest filings (updated every 10 minutes during working hours), and the same for ESEF filings. Data can be stored in `sqlite`, `postgres` or `mysql` databases (`sqlite` recommended).

A downloadable sqlite example database is available on google drive [here](https://drive.google.com/uc?id=1U5ch8G7DkdhbS1wBtXob8qxwZ0N6U1hF&export=download), contains filings data until 2022-09-14.

# Installation
```bash
pip install xbrl-reports-index
```
## From source
Clone [repo](https://github.com/selgamal/xbrl-reports-indexes), then build and install locally or install editable:
```bash
# editable
pip install -e .
# build
python -m build
pip install dist/*.whl
```

# Usage

## Command line
For options and help:
```
$ xri-db-tasks -h
```
```bash
# initialize sqlite database update data (requires internet connection)
$ xri-db-task <database-name> --initialize-database --update-sec --update-sec
```
Initializing the database and loading data may take a long time for SEC information, but subsequent updates should take less than a minute. when using `postgres` or `mysql`, database must be created on the server first and connection information should be provided as follows:
```bash
$ xri-db-task database_name,product,user,password,host,port,timeout --initialize-database --update-sec --update-sec
```
`product`: sqlite (default), postgres or mysql.

To search filings:
```bash
$ xri-db-tasks <db connection params> --search-filings sec --form-type 10-q --added-date-from 2022-08-10 --added-date-to 2022-08-10 --sec-industry-tree 70 --limit-result 1000 --output-file search.rss
2022-09-14 03:48:32,220 [rss-db.initialize-db] Verified/Initialized 
2022-09-14 03:48:33,269 [rss-db.info] Created search.rss for 186 filings.
```
The above command returns SEC filings for forms 10-K and 10-Q filed between 2022-08-10 and 2022-08-12 inclusive, result is saved as an rss file similar to SEC rss feed, this file can be used by `arelle` for further processing.

## Python script
This package uses [SQLAlchemy](https://github.com/sqlalchemy/sqlalchemy) as an [`ORM`](https://en.wikipedia.org/wiki/Object%E2%80%93relational_mapping). Usage is as follows:
```python
from xbrlreportsindexes.core.index_db import XbrlIndexDB
from sqlalchemy.orm import Session

# create a db object
db = XbrlIndexDB('scratches/xdb_test.db')

# create a query object
qry = db.search_filings(
        filing_system='sec',
        publication_date_from='2022-08-10',
        publication_date_to='2022-08-12',
        form_type='10-q',
        industry_code_tree='70',
        limit=1000,
)

# execute query and print out count
with Session(db.engine) as session:
    result = qry.with_session(session)
    print(result.count())
```
Result:
```
>>> 186
```
Or we can go directly to the industry table:

```python
# import SEC model
from xbrlreportsindexes.model import SEC

session = Session(db.engine)
# filter for industry 70
industry = session.query(
    SEC.SecIndustry).filter(
        SEC.SecIndustry.industry_classification=='SEC',
        SEC.SecIndustry.industry_code==70,
        ).first()
print(industry.industry_description)
```
industry name
```
>>> 'Services'
```
```python
# get filings of that industry
filings = industry.all_industry_tree_filings(session)

# filter filings
filings.filter(
    SEC.SecFiling.pub_date >= '2022-08-10',
    SEC.SecFiling.pub_date < '2022-08-13',
    SEC.SecFiling.form_type.like('%10-Q%'),
    ).count()
```
result of count
```
>>> 186
```
All other tools from `SQLAlchemy` can be used to query and analyze the data, for example, here is the to 5 forms by count of filings for August 2022:

```python
from sqlalchemy import func, desc

form_type = SEC.SecFiling.form_type

filings_august_qry = session.query(
    form_type, func.count(form_type).label('count_forms')).group_by(
        form_type).order_by(desc('count_forms')).limit(5)

for f in filings_august_qry:
    print(f[0], '->', f'{f[1]:,}', 'forms')
```
```
8-K -> 8,193 forms
10-Q -> 5,343 forms
8-K/A -> 198 forms
485BPOS -> 173 forms
10-K -> 167 forms
```
## Data update
A cron job can be setup to collect information about new filing from SEC site every 10 minutes during working hours as follows:
```bash
*/10 6-22 * * 1-5 xri-db-task <db connection parameters> --update-sec # note timezone differences
```
# License
Apache-2.0
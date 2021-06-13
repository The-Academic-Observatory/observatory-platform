# Copyright 2020 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: James Diprose

from __future__ import annotations

import logging
import os
import random
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Dict
from typing import List

import pandas as pd
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.dummy_operator import DummyOperator
from click.testing import CliRunner
from faker import Faker

from observatory.dags.workflows.doi import DoiWorkflow, make_dataset_transforms
from observatory.platform.utils.airflow_utils import set_task_state
from observatory.platform.utils.gc_utils import bigquery_sharded_table_id, load_bigquery_table, SourceFormat
from observatory.platform.utils.gc_utils import run_bigquery_query
from observatory.platform.utils.gc_utils import upload_files_to_cloud_storage
from observatory.platform.utils.telescope_utils import list_to_jsonl_gz
from observatory.platform.utils.telescope_utils import load_jsonl
from observatory.platform.utils.template_utils import schema_path, find_schema
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
    test_fixtures_path,
)

LICENSES = ["cc-by", None]

EVENT_TYPES = [
    "f1000",
    "stackexchange",
    "datacite",
    "twitter",
    "reddit-links",
    "wordpressdotcom",
    "plaudit",
    "cambia-lens",
    "hypothesis",
    "wikipedia",
    "reddit",
    "crossref",
    "newsfeed",
    "web",
]

OUTPUT_TYPES = [
    "journal_articles",
    "book_sections",
    "authored_books",
    "edited_volumes",
    "reports",
    "datasets",
    "proceedings_article",
    "other_outputs",
]

FUNDREF_COUNTRY_CODES = ["usa", "gbr", "aus", "can"]

FUNDREF_REGIONS = {"usa": "Americas", "gbr": "Europe", "aus": "Oceania", "can": "Americas"}

FUNDING_BODY_TYPES = [
    "For-profit companies (industry)",
    "Trusts, charities, foundations (both public and private)",
    "Associations and societies (private and public)",
    "National government",
    "Universities (academic only)",
    "International organizations",
    "Research institutes and centers",
    "Other non-profit organizations",
    "Local government",
    "Libraries and data archiving organizations",
]

FUNDING_BODY_SUBTYPES = {
    "For-profit companies (industry)": "pri",
    "Trusts, charities, foundations (both public and private)": "pri",
    "Associations and societies (private and public)": "pri",
    "National government": "gov",
    "Universities (academic only)": "gov",
    "International organizations": "pri",
    "Research institutes and centers": "pri",
    "Other non-profit organizations": "pri",
    "Local government": "gov",
    "Libraries and data archiving organizations": "gov",
}


def make_dummy_dag(dag_id: str, execution_date: datetime):
    with DAG(
        dag_id=dag_id,
        schedule_interval="@weekly",
        default_args={"owner": "airflow", "start_date": execution_date},
        catchup=False,
    ) as dag:
        task1 = DummyOperator(task_id="dummy_task")

    return dag


@dataclass
class Institution:
    id: int
    name: str = None
    grid_id: str = None
    country_code: str = None
    country_code_2: str = None
    region: str = None
    subregion: str = None
    papers: List[Paper] = None
    types: str = None
    home_repo: str = None
    country: str = None
    coordinates: str = None


@dataclass
class Paper:
    id: int
    doi: str = None
    title: str = None
    published_date: pendulum.Date = None
    output_type: str = None
    authors: List[Author] = None
    funders: List[Funder] = None
    journal: Journal = None
    publisher: Publisher = None
    events: List[Event] = None
    cited_by: List[Paper] = None
    fields_of_study: List[FieldOfStudy] = None
    license: str = None
    is_free_to_read_at_publisher: bool = False
    is_in_institutional_repo: bool = False

    @property
    def access_type(self) -> AccessType:
        gold_doaj = self.journal.license is not None
        gold = gold_doaj or (self.is_free_to_read_at_publisher and self.license is not None and not gold_doaj)
        hybrid = self.is_free_to_read_at_publisher and self.license is not None and not gold_doaj
        bronze = self.is_free_to_read_at_publisher and self.license is None and not gold_doaj
        green = self.is_in_institutional_repo
        green_only = self.is_in_institutional_repo and not gold_doaj and not self.is_free_to_read_at_publisher
        oa = gold or hybrid or bronze or green

        return AccessType(
            oa=oa, green=green, gold=gold, gold_doaj=gold_doaj, hybrid=hybrid, bronze=bronze, green_only=green_only
        )


@dataclass
class AccessType:
    oa: bool = None
    green: bool = None
    gold: bool = None
    gold_doaj: bool = None
    hybrid: bool = None
    bronze: bool = None
    green_only: bool = None


@dataclass
class Author:
    id: int
    name: str = None
    institution: Institution = None


@dataclass
class Funder:
    id: int
    name: str = None
    doi: str = None
    country_code: str = None
    region: str = None
    funding_body_type: str = None
    funding_body_subtype: str = None


@dataclass
class Publisher:
    id: int
    name: str = None
    doi_prefix: int = None
    journals: List[Journal] = None


@dataclass
class FieldOfStudy:
    id: int
    name: str = None
    level: int = None


@dataclass
class Journal:
    id: int
    name: str = None
    license: str = None


@dataclass
class Event:
    source: str = None
    event_date: pendulum.Pendulum = None


@dataclass
class ObservatoryDataset:
    institutions: List[Institution]
    authors: List[Author]
    funders: List[Funder]
    publishers: List[Publisher]
    papers: List[Paper]
    fields_of_study: List[FieldOfStudy]


def make_doi(doi_prefix: int):
    return f"10.{doi_prefix}/{str(uuid.uuid4())}"


def make_observatory_dataset(
    n_funders: int = 5,
    n_publishers: int = 5,
    n_authors: int = 10,
    n_papers: int = 50,
    n_fields_of_study_per_level: int = 5,
) -> ObservatoryDataset:
    faker = Faker()

    # Institutions
    inst_curtin = Institution(
        1,
        name="Curtin University",
        grid_id="grid.1032.0",
        country_code="AUS",
        country_code_2="AU",
        region="Oceania",
        subregion="Australia and New Zealand",
        types="Education",
        home_repo="curtin.edu.au",
        country="Australia",
        coordinates="-32.005931, 115.894397",
    )
    inst_anu = Institution(
        2,
        name="Australian National University",
        grid_id="grid.1001.0",
        country_code="AUS",
        country_code_2="AU",
        region="Oceania",
        subregion="Australia and New Zealand",
        types="Education",
        home_repo="anu.edu.au",
        country="Australia",
        coordinates="-35.2778, 149.1205",
    )
    inst_akl = Institution(
        3,
        name="University of Auckland",
        grid_id="grid.9654.e",
        country_code="NZL",
        country_code_2="NZ",
        region="Oceania",
        subregion="Australia and New Zealand",
        types="Education",
        home_repo="auckland.ac.nz",
        country="New Zealand",
        coordinates="-36.852304, 174.767734",
    )
    institutions = [inst_curtin, inst_anu, inst_akl]

    # Funders
    funders = []
    doi_prefix = 1000
    for i, _ in enumerate(range(n_funders)):
        country_code = random.choice(FUNDREF_COUNTRY_CODES)
        funding_body_type = random.choice(FUNDING_BODY_TYPES)
        funders.append(
            Funder(
                i,
                name=faker.company(),
                doi=make_doi(doi_prefix),
                country_code=country_code,
                region=FUNDREF_REGIONS[country_code],
                funding_body_type=funding_body_type,
                funding_body_subtype=FUNDING_BODY_SUBTYPES[funding_body_type],
            )
        )
        doi_prefix += 1

    # Publishers and journals
    journals = []
    publishers = []
    journal_id = 0
    for i, _ in enumerate(range(n_publishers)):
        n_journals_ = random.randint(1, 3)
        journals_ = []
        for _ in range(n_journals_):
            journals_.append(Journal(journal_id, name=faker.company(), license=random.choice(LICENSES)))
            journal_id += 1

        publishers.append(Publisher(i, name=faker.company(), doi_prefix=doi_prefix, journals=journals_))
        journals += journals_
        doi_prefix += 1

    # Fields of study
    fields_of_study = []
    fos_id_ = 0
    for level in range(6):
        for _ in range(n_fields_of_study_per_level):
            n_words_ = random.randint(1, 3)
            name_ = faker.sentence(nb_words=n_words_)
            fos_ = FieldOfStudy(fos_id_, name=name_, level=level)
            fields_of_study.append(fos_)
            fos_id_ += 1

    # Authors
    authors = []
    for i, _ in enumerate(range(n_authors)):
        author = Author(i, name=faker.name(), institution=random.choice(institutions))
        authors.append(author)

    papers = []
    for i, _ in enumerate(range(n_papers)):
        # Random title
        n_words_ = random.randint(2, 10)
        title_ = faker.sentence(nb_words=n_words_)

        # Random date
        published_date_ = pendulum.strptime(faker.date(), "%Y-%m-%d").date()
        published_date_ = pendulum.date(year=published_date_.year, month=published_date_.month, day=published_date_.day)

        # Output type
        output_type_ = random.choice(OUTPUT_TYPES)

        # Pick a random list of authors
        n_authors_ = random.randint(1, 10)
        authors_ = random.sample(authors, n_authors_)

        # Random funder
        n_funders_ = random.randint(0, 3)
        if n_funders_ > 0:
            funders_ = random.sample(funders, n_funders_)
        else:
            funders_ = []

        # Random publisher
        publisher_ = random.choice(publishers)

        # Journal
        journal_ = random.choice(publisher_.journals)

        # Random DOI
        doi_ = make_doi(publisher_.doi_prefix)

        # Random events
        n_events_ = random.randint(0, 100)
        events_ = []
        for _ in range(n_events_):
            range_months_ = random.randint(1, min(24, max((pendulum.date.today() - published_date_).in_months(), 1)))
            date_end = published_date_.add(months=range_months_)
            event_date_ = faker.date_between_dates(date_start=published_date_, date_end=date_end)
            event_date_ = pendulum.datetime(year=event_date_.year, month=event_date_.month, day=event_date_.day)
            events_.append(Event(source=random.choice(EVENT_TYPES), event_date=event_date_))

        # Fields of study
        n_fos_ = random.randint(1, 20)
        level_0_index = 199
        fields_of_study_ = [random.choice(fields_of_study[:level_0_index])] + random.sample(fields_of_study, n_fos_)

        # Open access status
        is_free_to_read_at_publisher_ = True
        if journal_.license is not None:
            # Gold
            license_ = journal_.license
        else:
            license_ = random.choice(LICENSES)
            if license_ is None:
                # Bronze: free to read on publisher website but no license
                is_free_to_read_at_publisher_ = bool(random.getrandbits(1))
            # Hybrid: license=True

        # Green: in a 'repository'
        is_in_institutional_repo_ = bool(random.getrandbits(1))
        # Green not bronze: Not free to read at publisher but in a 'repository'

        # Make paper
        paper = Paper(
            i,
            doi=doi_,
            title=title_,
            published_date=published_date_,
            output_type=output_type_,
            authors=authors_,
            funders=funders_,
            journal=journal_,
            publisher=publisher_,
            events=events_,
            fields_of_study=fields_of_study_,
            license=license_,
            is_free_to_read_at_publisher=is_free_to_read_at_publisher_,
            is_in_institutional_repo=is_in_institutional_repo_,
        )
        papers.append(paper)

    # Create paper citations
    # Sort from oldest to newest
    papers.sort(key=lambda p: p.published_date)

    for i, paper in enumerate(papers):
        # Create cited_by
        n_papers_forwards = len(papers) - i
        n_cited_by = random.randint(0, int(n_papers_forwards / 2))
        paper.cited_by = random.sample(papers[i + 1 :], n_cited_by)

    return ObservatoryDataset(institutions, authors, funders, publishers, papers, fields_of_study)


def make_open_citations(dataset: ObservatoryDataset) -> List[Dict]:
    records = []

    def make_oc_timespan(cited_date: pendulum.Date, citing_date: pendulum.Date):
        ts = "P"
        delta = citing_date - cited_date
        years = delta.in_years()
        months = delta.in_months() - years * 12

        if years > 0:
            ts += f"{years}Y"

        if months > 0 or years == 0:
            ts += f"{months}M"

        return ts

    def is_author_sc(cited_: Paper, citing_: Paper):
        for cited_author in cited_.authors:
            for citing_author in citing_.authors:
                if cited_author.name == citing_author.name:
                    return True
        return False

    def is_journal_sc(cited_: Paper, citing_: Paper):
        return cited_.journal.name == citing_.journal.name

    for cited in dataset.papers:
        for citing in cited.cited_by:
            records.append(
                {
                    "oci": "",
                    "citing": citing.doi,
                    "cited": cited.doi,
                    "creation": citing.published_date.strftime("%Y-%m"),
                    "timespan": make_oc_timespan(cited.published_date, citing.published_date),
                    "journal_sc": is_author_sc(cited, citing),
                    "author_sc": is_journal_sc(cited, citing),
                }
            )

    return records


def make_crossref_events(dataset: ObservatoryDataset) -> List[Dict]:
    events = []

    for paper in dataset.papers:
        for event in paper.events:
            obj_id = f"https://doi.org/{paper.doi}"
            occurred_at = f"{event.event_date.to_datetime_string()} UTC"
            source_id = event.source
            events.append(
                {"obj_id": obj_id, "occurred_at": occurred_at, "source_id": source_id, "id": str(uuid.uuid4())}
            )

    return events


def make_unpaywall(dataset: ObservatoryDataset) -> List[Dict]:
    records = []
    genre_lookup = {
        "journal_articles": ["journal-article"],
        "book_sections": ["book-section", "book-part", "book-chapter"],
        "authored_books": ["book", "monograph"],
        "edited_volumes": ["edited-book"],
        "reports": ["report"],
        "datasets": ["dataset"],
        "proceedings_article": ["proceedings-article"],
        "other_outputs": ["other-outputs"],
    }

    for paper in dataset.papers:
        # Make OA status
        journal_is_in_doaj = paper.journal.license is not None

        oa_locations = []
        if paper.is_free_to_read_at_publisher:
            oa_location = {"host_type": "publisher", "license": paper.license, "url": ""}
            oa_locations.append(oa_location)

        if paper.is_in_institutional_repo:
            oa_location = {"host_type": "repository", "license": paper.license, "url": ""}
            oa_locations.append(oa_location)

        is_oa = len(oa_locations) > 0
        if is_oa:
            best_oa_location = oa_locations[0]
        else:
            best_oa_location = None

        # Create record
        records.append(
            {
                "doi": paper.doi,
                "year": paper.published_date.year,
                "genre": random.choice(genre_lookup[paper.output_type]),
                "publisher": paper.publisher.name,
                "journal_name": paper.journal.name,
                "is_oa": is_oa,
                "journal_is_in_doaj": journal_is_in_doaj,
                "best_oa_location": best_oa_location,
                "oa_locations": oa_locations,
            }
        )

    return records


@dataclass
class MagDataset:
    affiliations: List[Dict]
    papers: List[Dict]
    paper_author_affiliations: List[Dict]
    fields_of_study: List[Dict]
    paper_fields_of_study: List[Dict]


def make_mag(dataset: ObservatoryDataset):
    # Create affiliations
    affiliations = []
    for institute in dataset.institutions:
        affiliations.append({"AffiliationId": institute.id, "DisplayName": institute.name, "GridId": institute.grid_id})

    # Create fields of study
    fields_of_study = []
    for fos in dataset.fields_of_study:
        fields_of_study.append({"FieldOfStudyId": fos.id, "DisplayName": fos.name, "Level": fos.level})

    # Create papers, paper_author_affiliations and paper_fields_of_study
    papers = []
    paper_author_affiliations = []
    paper_fields_of_study = []
    for paper in dataset.papers:
        papers.append({"PaperId": paper.id, "CitationCount": len(paper.cited_by), "Doi": paper.doi})

        for author in paper.authors:
            paper_author_affiliations.append(
                {"PaperId": paper.id, "AuthorId": author.id, "AffiliationId": author.institution.id}
            )

        for fos in paper.fields_of_study:
            paper_fields_of_study.append({"PaperId": paper.id, "FieldOfStudyId": fos.id})

    return MagDataset(affiliations, papers, paper_author_affiliations, fields_of_study, paper_fields_of_study)


def make_fundref(dataset: ObservatoryDataset) -> List[Dict]:
    records = []

    for funder in dataset.funders:
        records.append(
            {
                "pre_label": funder.name,
                "funder": f"http://dx.doi.org/{funder.doi}",
                "country_code": funder.country_code,
                "region": funder.region,
                "funding_body_type": funder.funding_body_type,
                "funding_body_sub_type": funder.funding_body_subtype,
            }
        )

    return records


def make_crossref_metadata(dataset: ObservatoryDataset) -> List[Dict]:
    records = []

    for paper in dataset.papers:
        # Create funders
        funders = []
        for funder in paper.funders:
            funders.append({"name": funder.name, "DOI": funder.doi, "award": None, "doi_asserted_by": None})

        # Add Crossref record
        records.append(
            {
                "title": [paper.title],
                "DOI": paper.doi,
                "is_referenced_by_count": len(paper.cited_by),
                "issued": {
                    "date_parts": [paper.published_date.year, paper.published_date.month, paper.published_date.day]
                },
                "funder": funders,
                "publisher": paper.publisher.name,
            }
        )

    return records


def make_orcid(dataset: ObservatoryDataset) -> List[Dict]:
    pass


def count_events(events: List[Event]):
    lookup_totals = dict()
    lookup_months = dict()
    lookup_years = dict()
    for event in events:
        # Total events
        if event.source in lookup_totals:
            lookup_totals[event.source] += 1
        else:
            lookup_totals[event.source] = 1

        # Events by month
        month = event.event_date.strftime("%Y-%m")
        month_key = (event.source, month)
        if month_key in lookup_months:
            lookup_months[month_key] += 1
        else:
            lookup_months[month_key] = 1

        # Events by year
        year = event.event_date.year
        year_key = (event.source, year)
        if year_key in lookup_years:
            lookup_years[year_key] += 1
        else:
            lookup_years[year_key] = 1

    total = [{"source": source, "count": count} for source, count in lookup_totals.items()]
    months = [{"source": source, "month": month, "count": count} for (source, month), count in lookup_months.items()]
    years = [{"source": source, "year": year, "count": count} for (source, year), count in lookup_years.items()]

    # Sort
    sort_events(total, months, years)
    return total, months, years


def sort_events(events, months, years):
    events.sort(key=lambda x: x["source"])
    months.sort(key=lambda x: f"{x['month']}{x['source']}{x['count']}")
    years.sort(key=lambda x: f"{x['year']}{x['source']}{x['count']}")


def make_doi_table(dataset: ObservatoryDataset) -> List[Dict]:
    records = []
    for paper in dataset.papers:
        doi = paper.doi.upper()
        events_total, events_months, events_years = count_events(paper.events)
        grids = list(set([author.institution.grid_id for author in paper.authors]))

        # When no events, events is None
        events = None
        if len(events_total):
            events = {
                "doi": doi,
                "events": events_total,
                "months": events_months,
                "years": events_years,
            }

        #
        # Make affiliations
        #

        # Institutions, countries, regions, subregion
        institutions = {}
        countries = {}
        regions = {}
        subregions = {}
        authors = []
        for author in paper.authors:
            # # Author
            # authors.append(
            #     {
            #         "identifier": author.orcid,
            #         "name": author.name,
            #         "given_names": author.first_name,
            #         "family_name": author.last_name,
            #         "types": ["Author"],
            #         "home_repo": None,
            #         "country": None,
            #         "country_code": None,
            #         "country_code_2": None,
            #         "region": None,
            #         "subregion": None,
            #         "coordinates": None,
            #         "members": [],
            #     }
            # )

            # Institution
            inst = author.institution
            if inst.grid_id not in institutions:
                institutions[inst.grid_id] = {
                    "identifier": inst.grid_id,
                    "types": [inst.types],
                    "name": inst.name,
                    "home_repo": {inst.home_repo},
                    "country": inst.country,
                    "country_code": inst.country_code,
                    "country_code_2": inst.country_code_2,
                    "region": inst.region,
                    "subregion": inst.subregion,
                    "coordinates": inst.coordinates,
                    "members": [],
                }

            # Country
            if inst.country not in countries:
                countries[inst.country] = {
                    "identifier": inst.country_code,
                    "name": inst.country,
                    "types": ["Country"],
                    "home_repo": {inst.home_repo},
                    "country": inst.country,
                    "country_code": inst.country_code,
                    "country_code_2": inst.country_code_2,
                    "region": inst.region,
                    "subregion": inst.subregion,
                    "coordinates": None,
                    "count": 0,
                    "members": {inst.grid_id},
                    "grids": {inst.grid_id},
                }
            else:
                countries[inst.country]["members"].add(inst.grid_id)
                countries[inst.country]["home_repo"].add(inst.home_repo)
                countries[inst.country]["grids"].add(inst.grid_id)

            # Region
            if inst.region not in regions:
                regions[inst.region] = {
                    "identifier": inst.region,
                    "name": inst.region,
                    "types": ["Region"],
                    "home_repo": {inst.home_repo},
                    "country": None,
                    "country_code": None,
                    "country_code_2": None,
                    "region": inst.region,
                    "subregion": None,
                    "coordinates": None,
                    "count": 0,
                    "members": {inst.subregion},
                    "grids": {inst.grid_id},
                }
            else:
                regions[inst.region]["members"].add(inst.subregion)
                regions[inst.region]["home_repo"].add(inst.home_repo)
                regions[inst.region]["grids"].add(inst.grid_id)

            if inst.subregion not in subregions:
                subregions[inst.subregion] = {
                    "identifier": inst.subregion,
                    "name": inst.subregion,
                    "types": ["Subregion"],
                    "home_repo": {inst.home_repo},
                    "country": None,
                    "country_code": None,
                    "country_code_2": None,
                    "region": inst.region,
                    "subregion": None,
                    "coordinates": None,
                    "count": 0,
                    "members": {inst.country_code},
                    "grids": {inst.grid_id},
                }
            else:
                subregions[inst.subregion]["members"].add(inst.country_code)
                subregions[inst.subregion]["home_repo"].add(inst.home_repo)
                subregions[inst.subregion]["grids"].add(inst.grid_id)

        def to_list(dict_: Dict):
            l_ = []
            for k, v in dict_.items():
                v["members"] = list(v["members"])
                v["home_repo"] = list(v["home_repo"])
                v["members"].sort()
                if "count" in v:
                    v["count"] = len(v["grids"])
                    v.pop("grids", None)
                v["home_repo"].sort()
                l_.append(v)
            l_.sort(key=lambda x: x["identifier"])
            return l_

        institutions = to_list(institutions)
        countries = to_list(countries)
        regions = to_list(regions)
        subregions = to_list(subregions)
        authors.sort(key=lambda x: x["identifier"])

        # Groupings

        # Funders
        funders = {}
        for funder in paper.funders:
            funders[funder.doi] = {
                "identifier": funder.name,
                "name": funder.name,
                "doi": funder.doi,
                "types": ["Funder"],
                "home_repo": [],
                "country": None,
                "country_code": funder.country_code,
                "country_code_2": None,
                "region": funder.region,
                "subregion": None,
                "coordinates": None,
                "funding_body_type": funder.funding_body_type,
                "funding_body_subtype": funder.funding_body_subtype,
                "members": [],
            }
        funders = [v for k, v in funders.items()]
        funders.sort(key=lambda x: x["identifier"])

        # Journals
        journal = paper.journal
        journals = [
            {
                "identifier": journal.name,
                "types": ["Journal"],
                "name": journal.name,
                "home_repo": [],
                "country": None,
                "country_code": None,
                "country_code_2": None,
                "region": None,
                "subregion": None,
                "coordinates": None,
                "members": [],
            }
        ]

        # Publishers
        publisher = paper.publisher
        publishers = [
            {
                "identifier": publisher.name,
                "types": ["Publisher"],
                "name": publisher.name,
                "home_repo": [],
                "country": None,
                "country_code": None,
                "country_code_2": None,
                "region": None,
                "subregion": None,
                "coordinates": None,
                "members": [],
            }
        ]

        #
        # Make final record
        #
        records.append(
            {
                "doi": doi,
                "crossref": {
                    "title": paper.title,
                    "published_year": paper.published_date.year,
                    "published_month": paper.published_date.month,
                    "published_year_month": f"{paper.published_date.year}-{paper.published_date.month}",
                    "funder": [{"name": funder.name, "DOI": funder.doi} for funder in paper.funders],
                },
                "unpaywall": {},
                "unpaywall_history": {},
                "mag": {},
                "open_citations": {},
                "events": events,
                "grids": grids,
                "affiliations": {
                    "doi": doi,
                    "institutions": institutions,
                    "countries": countries,
                    "subregions": subregions,
                    "regions": regions,
                    "groupings": [],
                    "funders": funders,
                    "authors": authors,
                    "journals": journals,
                    "publishers": publishers,
                },
            }
        )

    # Sort to match with sorted results
    records.sort(key=lambda r: r["doi"])

    return records


def make_country_table(dataset: ObservatoryDataset) -> List[Dict]:
    data = []
    for paper in dataset.papers:
        for author in paper.authors:
            inst = author.institution
            at = paper.access_type
            data.append(
                {
                    "doi": paper.doi,
                    "id": inst.country_code,
                    "time_period": paper.published_date.year,
                    "name": inst.country,
                    "home_repo": None,
                    "country": inst.country,
                    "country_code": inst.country_code,
                    "country_code_2": inst.country_code_2,
                    "region": inst.region,
                    "subregion": inst.subregion,
                    "coordinates": None,
                    "total_outputs": 1,
                    "oa": at.oa,
                    "green": at.green,
                    "gold": at.gold,
                    "gold_doaj": at.gold_doaj,
                    "hybrid": at.hybrid,
                    "bronze": at.bronze,
                    "green_only": at.green_only,
                }
            )

    df = pd.DataFrame(data)
    df.drop_duplicates(inplace=True)
    agg = {
        "id": "first",
        "time_period": "first",
        "name": "first",
        "home_repo": "first",
        "country": "first",
        "country_code": "first",
        "country_code_2": "first",
        "region": "first",
        "subregion": "first",
        "coordinates": "first",
        "total_outputs": "sum",
        "oa": "sum",
        "green": "sum",
        "gold": "sum",
        "gold_doaj": "sum",
        "hybrid": "sum",
        "bronze": "sum",
        "green_only": "sum",
    }
    df = df.groupby(["id", "time_period"], as_index=False).agg(agg).sort_values(by=["id", "time_period"])
    records = []
    for i, row in df.iterrows():
        total_outputs = row["total_outputs"]
        oa = row["oa"]
        green = row["green"]
        gold = row["gold"]
        gold_doaj = row["gold_doaj"]
        hybrid = row["hybrid"]
        bronze = row["bronze"]
        green_only = row["green_only"]

        records.append(
            {
                "id": row["id"],
                "time_period": row["time_period"],
                "name": row["name"],
                "home_repo": row["home_repo"],
                "country": row["country"],
                "country_code": row["country_code"],
                "country_code_2": row["country_code_2"],
                "region": row["region"],
                "subregion": row["subregion"],
                "coordinates": row["coordinates"],
                "total_outputs": total_outputs,
                "access_types": {
                    "oa": {"total_outputs": oa, "percent": round(oa / total_outputs * 100, 2)},
                    "green": {"total_outputs": green, "percent": round(green / total_outputs * 100, 2)},
                    "gold": {"total_outputs": gold, "percent": round(gold / total_outputs * 100, 2)},
                    "gold_doaj": {"total_outputs": gold_doaj, "percent": round(gold_doaj / total_outputs * 100, 2)},
                    "hybrid": {"total_outputs": hybrid, "percent": round(hybrid / total_outputs * 100, 2)},
                    "bronze": {"total_outputs": bronze, "percent": round(bronze / total_outputs * 100, 2)},
                    "green_only": {"total_outputs": green_only, "percent": round(green_only / total_outputs * 100, 2)},
                },
                "citations": {},
                "output_types": [],
                "disciplines": {},
                "funders": [],
                "members": [],
                "publishers": [],
                "journals": [],
                "events": [],
            }
        )

    return records


class TestDoiWorkflow(ObservatoryTestCase):
    """ Tests for the functions used by the Doi workflow """

    def __init__(self, *args, **kwargs):
        super(TestDoiWorkflow, self).__init__(*args, **kwargs)
        self.gcp_project_id: str = os.getenv("TEST_GCP_PROJECT_ID")
        self.gcp_bucket_name: str = os.getenv("TEST_GCP_BUCKET_NAME")
        self.gcp_data_location: str = os.getenv("TEST_GCP_DATA_LOCATION")

    def test_set_task_state(self):
        set_task_state(True, "my-task-id")
        with self.assertRaises(AirflowException):
            set_task_state(False, "my-task-id")

    def test_dag_structure(self):
        """Test that the DOI DAG has the correct structure.

        :return: None
        """

        dag = DoiWorkflow().make_dag()
        self.assert_dag_structure(
            {
                "crossref_metadata_sensor": ["check_dependencies"],
                "fundref_sensor": ["check_dependencies"],
                "geonames_sensor": ["check_dependencies"],
                "grid_sensor": ["check_dependencies"],
                "mag_sensor": ["check_dependencies"],
                "open_citations_sensor": ["check_dependencies"],
                "unpaywall_sensor": ["check_dependencies"],
                "check_dependencies": ["create_datasets"],
                "create_datasets": [
                    "extend_grid",
                    "aggregate_crossref_events",
                    "aggregate_orcid",
                    "aggregate_mag",
                    "aggregate_unpaywall",
                    "extend_crossref_funders",
                    "aggregate_open_citations",
                    "aggregate_wos",
                    "aggregate_scopus",
                ],
                "extend_grid": ["create_doi"],
                "aggregate_crossref_events": ["create_doi"],
                "aggregate_orcid": ["create_doi"],
                "aggregate_mag": ["create_doi"],
                "aggregate_unpaywall": ["create_doi"],
                "extend_crossref_funders": ["create_doi"],
                "aggregate_open_citations": ["create_doi"],
                "aggregate_wos": ["create_doi"],
                "aggregate_scopus": ["create_doi"],
                "create_doi": [
                    "create_country",
                    "create_funder",
                    "create_group",
                    "create_institution",
                    "create_author",
                    "create_journal",
                    "create_publisher",
                    "create_region",
                    "create_subregion",
                ],
                "create_country": ["copy_to_dashboards"],
                "create_funder": ["copy_to_dashboards"],
                "create_group": ["copy_to_dashboards"],
                "create_institution": ["copy_to_dashboards"],
                "create_author": ["copy_to_dashboards"],
                "create_journal": ["copy_to_dashboards"],
                "create_publisher": ["copy_to_dashboards"],
                "create_region": ["copy_to_dashboards"],
                "create_subregion": ["copy_to_dashboards"],
                "copy_to_dashboards": ["create_dashboard_views"],
                "create_dashboard_views": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the DOI can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(self.gcp_project_id, self.gcp_data_location)
        with env.create():
            dag_file = os.path.join(module_file_path("observatory.dags.dags"), "doi.py")
            self.assert_dag_load("doi", dag_file)

    def setup_fake_observatory_dataset(
        self,
        observatory_dataset: ObservatoryDataset,
        bucket_name: str,
        dataset_id_all: str,
        dataset_id_settings: str,
        release_date: pendulum.Pendulum,
    ):
        # Generate source datasets
        open_citations = make_open_citations(observatory_dataset)
        crossref_events = make_crossref_events(observatory_dataset)
        mag: MagDataset = make_mag(observatory_dataset)
        fundref = make_fundref(observatory_dataset)
        unpaywall = make_unpaywall(observatory_dataset)
        crossref_metadata = make_crossref_metadata(observatory_dataset)

        # Load fake GRID and settings datasets
        test_doi_path = test_fixtures_path("telescopes", "doi")
        grid = load_jsonl(os.path.join(test_doi_path, "grid.jsonl"))
        iso3166_countries_and_regions = load_jsonl(os.path.join(test_doi_path, "iso3166_countries_and_regions.jsonl"))
        grid_to_home_url = load_jsonl(os.path.join(test_doi_path, "grid_to_home_url.jsonl"))
        groupings = load_jsonl(os.path.join(test_doi_path, "groupings.jsonl"))
        mag_affiliation_override = load_jsonl(os.path.join(test_doi_path, "mag_affiliation_override.jsonl"))

        with CliRunner().isolated_filesystem() as t:
            records = [
                ("crossref_events", False, dataset_id_all, crossref_events, "crossref_events"),
                ("crossref_metadata", True, dataset_id_all, crossref_metadata, "crossref_metadata"),
                ("fundref", True, dataset_id_all, fundref, "fundref"),
                ("Affiliations", True, dataset_id_all, mag.affiliations, "MagAffiliations"),
                ("FieldsOfStudy", True, dataset_id_all, mag.fields_of_study, "MagFieldsOfStudy"),
                (
                    "PaperAuthorAffiliations",
                    True,
                    dataset_id_all,
                    mag.paper_author_affiliations,
                    "MagPaperAuthorAffiliations",
                ),
                ("PaperFieldsOfStudy", True, dataset_id_all, mag.paper_fields_of_study, "MagPaperFieldsOfStudy"),
                ("Papers", True, dataset_id_all, mag.papers, "MagPapers"),
                ("open_citations", True, dataset_id_all, open_citations, "open_citations"),
                ("unpaywall", True, dataset_id_all, unpaywall, "unpaywall"),
                ("grid", True, dataset_id_all, grid, "grid"),
                (
                    "iso3166_countries_and_regions",
                    False,
                    dataset_id_all,
                    iso3166_countries_and_regions,
                    "iso3166_countries_and_regions",
                ),
                ("grid_to_home_url", False, dataset_id_settings, grid_to_home_url, "grid_to_home_url"),
                ("groupings", False, dataset_id_settings, groupings, "groupings"),
                (
                    "mag_affiliation_override",
                    False,
                    dataset_id_settings,
                    mag_affiliation_override,
                    "mag_affiliation_override",
                ),
                ("PaperAbstractsInvertedIndex", True, dataset_id_all, [], "MagPaperAbstractsInvertedIndex"),
                ("Journals", True, dataset_id_all, [], "MagJournals"),
                ("ConferenceInstances", True, dataset_id_all, [], "MagConferenceInstances"),
                ("ConferenceSeries", True, dataset_id_all, [], "MagConferenceSeries"),
                ("FieldOfStudyExtendedAttributes", True, dataset_id_all, [], "MagFieldOfStudyExtendedAttributes"),
                ("PaperExtendedAttributes", True, dataset_id_all, [], "MagPaperExtendedAttributes"),
                ("PaperResources", True, dataset_id_all, [], "MagPaperResources"),
                ("PaperUrls", True, dataset_id_all, [], "MagPaperUrls"),
                ("PaperMeSH", True, dataset_id_all, [], "MagPaperMeSH"),
                ("orcid", False, dataset_id_all, [], "orcid"),
            ]

            files_list = []
            blob_names = []

            # Save to JSONL
            for table_name, is_sharded, dataset_id, dataset, schema in records:
                blob_name = f"{table_name}.jsonl.gz"
                file_path = os.path.join(t, blob_name)
                list_to_jsonl_gz(file_path, dataset)
                files_list.append(file_path)
                blob_names.append(blob_name)

            # Upload to Google Cloud Storage
            success = upload_files_to_cloud_storage(bucket_name, blob_names, files_list)
            self.assertTrue(success)

            # Save to BigQuery tables
            for blob_name, (table_name, is_sharded, dataset_id, dataset, schema) in zip(blob_names, records):
                if is_sharded:
                    table_id = bigquery_sharded_table_id(table_name, release_date)
                else:
                    table_id = table_name

                # Select schema file based on release date
                analysis_schema_path = schema_path()
                schema_file_path = find_schema(analysis_schema_path, schema, release_date)
                if schema_file_path is None:
                    logging.error(
                        f"No schema found with search parameters: analysis_schema_path={analysis_schema_path}, "
                        f"table_name={table_name}, release_date={release_date}"
                    )
                    exit(os.EX_CONFIG)

                # Load BigQuery table
                uri = f"gs://{bucket_name}/{blob_name}"
                logging.info(f"URI: {uri}")
                success = load_bigquery_table(
                    uri,
                    dataset_id,
                    self.gcp_data_location,
                    table_id,
                    schema_file_path,
                    SourceFormat.NEWLINE_DELIMITED_JSON,
                )
                if not success:
                    raise AirflowException("bq_load task: data failed to load data into BigQuery")

    def test_telescope(self):
        """Test the DOI telescope end to end.

        :return: None.
        """

        # Create datasets
        env = ObservatoryEnvironment(project_id=self.gcp_project_id, data_location=self.gcp_data_location)
        fake_dataset_id = env.add_dataset(prefix="fake")
        intermediate_dataset_id = env.add_dataset(prefix="intermediate")
        dashboards_dataset_id = env.add_dataset(prefix="dashboards")
        observatory_dataset_id = env.add_dataset(prefix="observatory")
        elastic_dataset_id = env.add_dataset(prefix="elastic")
        settings_dataset_id = env.add_dataset(prefix="settings")
        dataset_transforms = make_dataset_transforms(
            dataset_id_crossref_events=fake_dataset_id,
            dataset_id_crossref_metadata=fake_dataset_id,
            dataset_id_fundref=fake_dataset_id,
            dataset_id_grid=fake_dataset_id,
            dataset_id_iso=fake_dataset_id,
            dataset_id_mag=fake_dataset_id,
            dataset_id_orcid=fake_dataset_id,
            dataset_id_open_citations=fake_dataset_id,
            dataset_id_unpaywall=fake_dataset_id,
            dataset_id_settings=settings_dataset_id,
            dataset_id_observatory=observatory_dataset_id,
            dataset_id_observatory_intermediate=intermediate_dataset_id,
        )
        transforms, transform_doi, transform_book = dataset_transforms

        with env.create():
            # Make dag
            start_date = pendulum.datetime(year=2021, month=5, day=9)
            doi_dag = DoiWorkflow(
                intermediate_dataset_id=intermediate_dataset_id,
                dashboards_dataset_id=dashboards_dataset_id,
                observatory_dataset_id=observatory_dataset_id,
                elastic_dataset_id=elastic_dataset_id,
                transforms=dataset_transforms,
                start_date=start_date,
            ).make_dag()

            # Test that sensors do go into the 'up_for_reschedule' state as the DAGs that they wait for haven't run
            # execution_date = pendulum.datetime(year=2021, month=5, day=9)
            expected_state = "up_for_reschedule"
            with env.create_dag_run(doi_dag, start_date):
                for task_id in DoiWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f"{task_id}_sensor", doi_dag, execution_date=start_date)
                    self.assertEqual(expected_state, ti.state)

            # Run Dummy Dags
            execution_date = pendulum.datetime(year=2021, month=5, day=16)
            release_date = pendulum.datetime(year=2021, month=5, day=22)
            expected_state = "success"
            for dag_id in DoiWorkflow.SENSOR_DAG_IDS:
                dag = make_dummy_dag(dag_id, execution_date)
                with env.create_dag_run(dag, execution_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    ti = env.run_task("dummy_task", dag, execution_date=execution_date)
                    self.assertEqual(expected_state, ti.state)

            # Run end to end tests for DOI DAG
            with env.create_dag_run(doi_dag, execution_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                for task_id in DoiWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f"{task_id}_sensor", doi_dag, execution_date=execution_date)
                    self.assertEqual(expected_state, ti.state)

                # Check dependencies
                ti = env.run_task("check_dependencies", doi_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)

                # Create datasets
                ti = env.run_task("create_datasets", doi_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)

                # Generate fake dataset
                observatory_dataset = make_observatory_dataset()
                self.setup_fake_observatory_dataset(
                    observatory_dataset, env.download_bucket, fake_dataset_id, settings_dataset_id, release_date
                )

                # Test that source dataset transformations run
                for transform in transforms:
                    task_id = f"create_{transform.output_table.table_id}"
                    ti = env.run_task(task_id, doi_dag, execution_date=execution_date)
                    self.assertEqual(expected_state, ti.state)

                # Test create DOI
                ti = env.run_task("create_doi", doi_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)
                expected_output = make_doi_table(observatory_dataset)
                actual_output = self.query_table(observatory_dataset_id, "doi", release_date, "doi")
                self.assert_doi(expected_output, actual_output)

                # Test create book
                ti = env.run_task("create_book", doi_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)
                # expected_table_id = f""
                # expected_rows = 5
                # self.assert_table_integrity(expected_table_id, expected_rows)

                # Test aggregations
                ti = env.run_task("create_country", doi_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)

                # Assert country aggregation output
                expected_output = make_country_table(observatory_dataset)
                actual_output = self.query_table(observatory_dataset_id, "country", release_date, "id, time_period")
                self.assert_aggregate(expected_output, actual_output)
                a = 1

                # for agg in DoiWorkflow.AGGREGATIONS:

                # Check that task finished successfully

                #     # TODO: check that output is correct

                # Test copy to dashboards
                ti = env.run_task("copy_to_dashboards", doi_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)
                # TODO: check that tables exist

                # Test create dashboard views
                ti = env.run_task("create_dashboard_views", doi_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)
                # TODO: check that views exist

                # Test create exported tables for Elasticsearch
                for agg in DoiWorkflow.AGGREGATIONS:
                    task_id = f"export_{agg.table_id}"
                    ti = env.run_task(task_id, doi_dag, execution_date=execution_date)

                    # Check that task finished successfully
                    self.assertEqual(expected_state, ti.state)
                    # TODO: check that output is correct
                #
                # a = 1

    def query_table(
        self, observatory_dataset_id: str, table_id: str, release_date: pendulum.Pendulum, order_by_field: str
    ):
        release_suffix = release_date.strftime("%Y%m%d")
        return [
            dict(row)
            for row in run_bigquery_query(
                f"SELECT * from {self.gcp_project_id}.{observatory_dataset_id}.{table_id}{release_suffix} ORDER BY {order_by_field} ASC;"
            )
        ]

    def assert_aggregate(self, expected: List[Dict], actual: List[Dict]):
        # Check that expected and actual are same length
        self.assertEqual(len(expected), len(actual))

        # Check that each item matches
        for expected_item, actual_item in zip(expected, actual):
            # Check that top level fields match
            for key in [
                "id",
                "time_period",
                "name",
                "country",
                "country_code",
                "country_code_2",
                "region",
                "subregion",
                "coordinates",
                "total_outputs",
            ]:
                self.assertEqual(expected_item[key], actual_item[key])

            # Access types
            self.assert_sub_fields(
                expected_item,
                actual_item,
                "access_types",
                ["oa", "green", "gold", "gold_doaj", "hybrid", "bronze", "green_only"],
            )

    def assert_sub_fields(self, expected: Dict, actual: Dict, field: str, sub_fields: List[str]):
        for key in sub_fields:
            self.assertEqual(expected[field][key], actual[field][key])

    def assert_doi(self, expected: List[Dict], actual: List[Dict]):
        # Assert DOI output is correct
        self.assertEqual(len(expected), len(actual))
        for expected_record, actual_record in zip(expected, actual):
            # Check that DOIs match
            self.assertEqual(expected_record["doi"], actual_record["doi"])

            # Check events
            self.assert_doi_events(expected_record["events"], actual_record["events"])

            # Check grids
            self.assertSetEqual(set(expected_record["grids"]), set(actual_record["grids"]))

            # Check affiliations
            self.assert_doi_affiliations(expected_record["affiliations"], actual_record["affiliations"])

    def assert_doi_events(self, expected: Dict, actual: Dict):
        if expected is None:
            # When no events exist assert they are None
            self.assertIsNone(actual)
        else:
            # When events exist check that they are equal
            self.assertEqual(expected["doi"], actual["doi"])
            sort_events(actual["events"], actual["months"], actual["years"])

            event_keys = ["events", "months", "years"]
            for key in event_keys:
                self.assertEqual(len(expected[key]), len(actual[key]))
                for ee, ea in zip(expected[key], actual[key]):
                    self.assertDictEqual(ee, ea)

    def assert_doi_affiliations(self, expected: Dict, actual: Dict):
        # DOI
        self.assertEqual(expected["doi"], actual["doi"])

        # Subfields
        fields = ["institutions", "countries", "subregions", "regions", "journals", "publishers", "funders"]
        for field in fields:
            self.assert_doi_affiliation(expected, actual, field)

    def assert_doi_affiliation(self, expected, actual, key):
        items_expected_ = expected[key]
        items_actual_ = actual[key]
        self.assertEqual(len(items_expected_), len(items_actual_))
        items_actual_.sort(key=lambda x: x["identifier"])
        for item_ in items_actual_:
            item_["home_repo"].sort()
            item_["members"].sort()
        self.assertListEqual(items_expected_, items_actual_)

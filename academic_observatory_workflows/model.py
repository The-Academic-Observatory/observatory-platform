# Copyright 2021 Curtin University
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

# Author: James Diprose, Tuan Chien

from __future__ import annotations

import os
import random
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd
import pendulum
from click.testing import CliRunner
from faker import Faker
from pendulum import DateTime

from academic_observatory_workflows.config import schema_folder, test_fixtures_folder
from observatory.platform.utils.file_utils import load_jsonl
from observatory.platform.utils.test_utils import Table, bq_load_tables

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


@dataclass
class Institution:
    """An institution.

    :param id: unique identifier.
    :param name: the institution's name.
    :param grid_id: the institution's GRID id.
    :param country_code: the institution's country code.
    :param country_code_2: the institution's country code.
    :param subregion: the institution's subregion.
    :param papers: the papers published by the institution.
    :param types: the institution type.
    :param home_repo: the institution type.
    :param country: the institution country name.
    :param coordinates: the institution's coordinates.
    """

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


def date_between_dates(start_ts: int, end_ts: int) -> DateTime:
    """Return a datetime between two timestamps.

    :param start_ts: the start timestamp.
    :param end_ts: the end timestamp.
    :return: the DateTime datetime.
    """

    r_ts = random.randint(start_ts, end_ts - 1)
    return pendulum.from_timestamp(r_ts)


@dataclass
class Paper:
    """A paper.

    :param id: unique identifier.
    :param doi: the DOI of the paper.
    :param title: the title of the paper.
    :param published_date: the date the paper was published.
    :param output_type: the output type, see OUTPUT_TYPES.
    :param authors: the authors of the paper.
    :param funders: the funders of the research published in the paper.
    :param journal: the journal this paper is published in.
    :param publisher: the publisher of this paper (the owner of the journal).
    :param events: a list of events related to this paper.
    :param cited_by: a list of papers that this paper is cited by.
    :param fields_of_study: a list of the fields of study of the paper.
    :param license:
    :param is_free_to_read_at_publisher:
    :param is_in_institutional_repo:
    """

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
        """Return the access type for the paper.

        :return: AccessType.
        """

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
    """The access type of a paper.

    :param oa: whether the paper is open access or not.
    :param green: when the paper is available in an institutional repository.
    :param gold: when the paper is an open access journal or (it is not in an open access journal and is free to read
    at the publisher and has an open access license).
    :param gold_doaj: when the paper is an open access journal.
    :param hybrid: where the paper is free to read at the publisher, it has an open access license and the journal is
    not open access.
    :param bronze: when the paper is free to read at the publisher website however there is no license.
    :param green_only: where the paper is not free to read from the publisher, however it is available at an
    institutional repository.
    """

    oa: bool = None
    green: bool = None
    gold: bool = None
    gold_doaj: bool = None
    hybrid: bool = None
    bronze: bool = None
    green_only: bool = None


@dataclass
class Author:
    """An author.

    :param id: unique identifier.
    :param name: the name of the author.
    :param institution: the author's institution.
    """

    id: int
    name: str = None
    institution: Institution = None


@dataclass
class Funder:
    """A research funder.

    :param id: unique identifier.
    :param name: the name of the funder.
    :param doi: the DOI of the funder.
    :param country_code: the country code of the funder.
    :param region: the region the funder is located in.
    :param funding_body_type: the funding body type, see FUNDING_BODY_TYPES.
    :param funding_body_subtype: the funding body subtype, see FUNDING_BODY_SUBTYPES.
    """

    id: int
    name: str = None
    doi: str = None
    country_code: str = None
    region: str = None
    funding_body_type: str = None
    funding_body_subtype: str = None


@dataclass
class Publisher:
    """A publisher.

    :param id: unique identifier.
    :param name: the name of the publisher.
    :param doi_prefix: the publisher DOI prefix.
    :param journals: the journals owned by the publisher.
    """

    id: int
    name: str = None
    doi_prefix: int = None
    journals: List[Journal] = None


@dataclass
class FieldOfStudy:
    """A field of study.

    :param id: unique identifier.
    :param name: the field of study name.
    :param level: the field of study level.
    """

    id: int
    name: str = None
    level: int = None


@dataclass
class Journal:
    """A journal

    :param id: unique identifier.
    :param name: the journal name.
    :param name: the license that articles are published under by the journal.
    """

    id: int
    name: str = None
    license: str = None


@dataclass
class Event:
    """An event.

    :param source: the source of the event, see EVENT_TYPES.
    :param event_date: the date of the event.
    """

    source: str = None
    event_date: DateTime = None


InstitutionList = List[Institution]
AuthorList = List[Author]
FunderList = List[Funder]
PublisherList = List[Publisher]
PaperList = List[Paper]
FieldOfStudyList = List[FieldOfStudy]
EventsList = List[Event]


@dataclass
class ObservatoryDataset:
    """The generated observatory dataset.

    :param institutions: list of institutions.
    :param authors: list of authors.
    :param funders: list of funders.
    :param publishers: list of publishers.
    :param papers: list of papers.
    :param fields_of_study: list of fields of study.
    """

    institutions: InstitutionList
    authors: AuthorList
    funders: FunderList
    publishers: PublisherList
    papers: PaperList
    fields_of_study: FieldOfStudyList


def make_doi(doi_prefix: int):
    """Makes a randomised DOI given a DOI prefix.

    :param doi_prefix: the DOI prefix.
    :return: the DOI.
    """

    return f"10.{doi_prefix}/{str(uuid.uuid4())}"


def make_observatory_dataset(
    institutions: List[Institution],
    n_funders: int = 5,
    n_publishers: int = 5,
    n_authors: int = 10,
    n_papers: int = 50,
    n_fields_of_study_per_level: int = 5,
) -> ObservatoryDataset:
    """Generate an observatory dataset.

    :param institutions: a list of institutions.
    :param n_funders: the number of funders to generate.
    :param n_publishers: the number of publishers to generate.
    :param n_authors: the number of authors to generate.
    :param n_papers: the number of papers to generate.
    :param n_fields_of_study_per_level: the number of fields of study to generate per level.
    :return: the observatory dataset.
    """

    faker = Faker()
    funder_doi_prefix = 1000
    funders = make_funders(n_funders=n_funders, doi_prefix=funder_doi_prefix, faker=faker)
    publisher_doi_prefix = funder_doi_prefix + len(funders)
    publishers = make_publishers(n_publishers=n_publishers, doi_prefix=publisher_doi_prefix, faker=faker)
    fields_of_study = make_fields_of_study(n_fields_of_study_per_level=n_fields_of_study_per_level, faker=faker)
    authors = make_authors(n_authors=n_authors, institutions=institutions, faker=faker)
    papers = make_papers(
        n_papers=n_papers,
        authors=authors,
        funders=funders,
        publishers=publishers,
        fields_of_study=fields_of_study,
        faker=faker,
    )

    return ObservatoryDataset(institutions, authors, funders, publishers, papers, fields_of_study)


def make_funders(*, n_funders: int, doi_prefix: int, faker: Faker) -> FunderList:
    """Make the funders ground truth dataset.

    :param n_funders: number of funders to generate.
    :param doi_prefix: the DOI prefix for the funders.
    :param faker: the faker instance.
    :return: a list of funders.
    """

    funders = []

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

    return funders


def make_publishers(
    *,
    n_publishers: int,
    doi_prefix: int,
    faker: Faker,
    min_journals_per_publisher: int = 1,
    max_journals_per_publisher: int = 3,
) -> PublisherList:
    """Make publishers ground truth dataset.

    :param n_publishers: number of publishers.
    :param doi_prefix: the publisher DOI prefix.
    :param faker: the faker instance.
    :param min_journals_per_publisher: the min number of journals to generate per publisher.
    :param max_journals_per_publisher: the max number of journals to generate per publisher.
    :return:
    """

    publishers = []
    journal_id = 0
    for i, _ in enumerate(range(n_publishers)):
        n_journals_ = random.randint(min_journals_per_publisher, max_journals_per_publisher)
        journals_ = []
        for _ in range(n_journals_):
            journals_.append(Journal(journal_id, name=faker.company(), license=random.choice(LICENSES)))
            journal_id += 1

        publishers.append(Publisher(i, name=faker.company(), doi_prefix=doi_prefix, journals=journals_))
        doi_prefix += 1

    return publishers


def make_fields_of_study(
    *,
    n_fields_of_study_per_level: int,
    faker: Faker,
    n_levels: int = 6,
    min_title_length: int = 1,
    max_title_length: int = 3,
) -> FieldOfStudyList:
    """Generate the fields of study for the ground truth dataset.

    :param n_fields_of_study_per_level: the number of fields of study per level.
    :param faker: the faker instance.
    :param n_levels: the number of levels.
    :param min_title_length: the minimum field of study title length (words).
    :param max_title_length: the maximum field of study title length (words).
    :return: a list of the fields of study.
    """

    fields_of_study = []
    fos_id_ = 0
    for level in range(n_levels):
        for _ in range(n_fields_of_study_per_level):
            n_words_ = random.randint(min_title_length, max_title_length)
            name_ = faker.sentence(nb_words=n_words_)
            fos_ = FieldOfStudy(fos_id_, name=name_, level=level)
            fields_of_study.append(fos_)
            fos_id_ += 1

    return fields_of_study


def make_authors(*, n_authors: int, institutions: InstitutionList, faker: Faker) -> AuthorList:
    """Generate the authors ground truth dataset.

    :param n_authors: the number of authors to generate.
    :param institutions: the institutions.
    :param faker: the faker instance.
    :return: a list of authors.
    """

    authors = []
    for i, _ in enumerate(range(n_authors)):
        author = Author(i, name=faker.name(), institution=random.choice(institutions))
        authors.append(author)

    return authors


def make_papers(
    *,
    n_papers: int,
    authors: AuthorList,
    funders: FunderList,
    publishers: PublisherList,
    fields_of_study: List,
    faker: Faker,
    min_title_length: int = 2,
    max_title_length: int = 10,
    min_authors: int = 1,
    max_authors: int = 10,
    min_funders: int = 0,
    max_funders: int = 3,
    min_events: int = 0,
    max_events: int = 100,
    min_fields_of_study: int = 1,
    max_fields_of_study: int = 20,
) -> PaperList:
    """Generate the list of ground truth papers.

    :param n_papers: the number of papers to generate.
    :param authors: the authors list.
    :param funders: the funders list.
    :param publishers: the publishers list.
    :param fields_of_study: the fields of study list.
    :param faker: the faker instance.
    :param min_title_length: the min paper title length.
    :param max_title_length: the max paper title length.
    :param min_authors: the min number of authors for each paper.
    :param max_authors: the max number of authors for each paper.
    :param min_funders: the min number of funders for each paper.
    :param max_funders: the max number of funders for each paper.
    :param min_events: the min number of events per paper.
    :param max_events: the max number of events per paper.
    :param min_fields_of_study: the min fields of study per paper.
    :param max_fields_of_study: the max fields of study per paper.
    :return: the list of papers.
    """

    papers = []

    for i, _ in enumerate(range(n_papers)):
        # Random title
        n_words_ = random.randint(min_title_length, max_title_length)
        title_ = faker.sentence(nb_words=n_words_)

        # Random date
        published_date_ = pendulum.from_format(faker.date(), "YYYY-MM-DD").date()
        published_date_ = pendulum.date(year=published_date_.year, month=published_date_.month, day=published_date_.day)

        # Output type
        output_type_ = random.choice(OUTPUT_TYPES)

        # Pick a random list of authors
        n_authors_ = random.randint(min_authors, max_authors)
        authors_ = random.sample(authors, n_authors_)

        # Random funder
        n_funders_ = random.randint(min_funders, max_funders)
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
        n_events_ = random.randint(min_events, max_events)
        events_ = []
        today = datetime.now()
        today_ts = int(today.timestamp())
        start_date = datetime(today.year - 2, today.month, today.day)
        start_ts = int(start_date.timestamp())

        for _ in range(n_events_):
            event_date_ = date_between_dates(start_ts=start_ts, end_ts=today_ts)
            events_.append(Event(source=random.choice(EVENT_TYPES), event_date=event_date_))

        # Fields of study
        n_fos_ = random.randint(min_fields_of_study, max_fields_of_study)
        level_0_index = 199
        fields_of_study_ = [random.choice(fields_of_study[:level_0_index])]
        fields_of_study_.extend(random.sample(fields_of_study, n_fos_))

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

    return papers


def make_open_citations(dataset: ObservatoryDataset) -> List[Dict]:
    """Generate an Open Citations table from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

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
    """Generate the Crossref Events table from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

    events = []

    for paper in dataset.papers:
        for event in paper.events:
            obj_id = f"https://doi.org/{paper.doi}"
            occurred_at = f"{event.event_date.to_datetime_string()} UTC"
            source_id = event.source
            events.append(
                {
                    "obj_id": obj_id,
                    "timestamp": occurred_at,
                    "occurred_at": occurred_at,
                    "source_id": source_id,
                    "id": str(uuid.uuid4()),
                }
            )

    return events


def make_unpaywall(dataset: ObservatoryDataset) -> List[Dict]:
    """Generate the Unpaywall table from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

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
    """A container to hold the Microsoft Academic Graph tables.

    :param: Affiliations table rows.
    :param: Papers table rows.
    :param: PaperAuthorAffiliations rows.
    :param: FieldsOfStudy rows.
    :param: PaperFieldsOfStudy rows.
    """

    affiliations: List[Dict]
    papers: List[Dict]
    paper_author_affiliations: List[Dict]
    fields_of_study: List[Dict]
    paper_fields_of_study: List[Dict]


def make_mag(dataset: ObservatoryDataset) -> MagDataset:
    """Generate the Microsoft Academic Graph tables from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: the Microsoft Academic Graph dataset.
    """

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


def make_crossref_fundref(dataset: ObservatoryDataset) -> List[Dict]:
    """Generate the Crossref Fundref table from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

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
    """Generate the Crossref Metadata table from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

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


def bq_load_observatory_dataset(
    observatory_dataset: ObservatoryDataset,
    bucket_name: str,
    dataset_id_all: str,
    dataset_id_settings: str,
    release_date: DateTime,
    data_location: str,
):
    """Load the fake Observatory Dataset in BigQuery.

    :param observatory_dataset: the Observatory Dataset.
    :param bucket_name: the Google Cloud Storage bucket name.
    :param dataset_id_all: the dataset id for all data tables.
    :param dataset_id_settings: the dataset id for settings tables.
    :param release_date: the release date for the observatory dataset.
    :param data_location: the location of the BigQuery dataset.
    :return: None.
    """

    # Generate source datasets
    open_citations = make_open_citations(observatory_dataset)
    crossref_events = make_crossref_events(observatory_dataset)
    mag: MagDataset = make_mag(observatory_dataset)
    crossref_fundref = make_crossref_fundref(observatory_dataset)
    unpaywall = make_unpaywall(observatory_dataset)
    crossref_metadata = make_crossref_metadata(observatory_dataset)

    # Load fake GRID and settings datasets
    test_doi_path = test_fixtures_folder("doi")
    grid = load_jsonl(os.path.join(test_doi_path, "grid.jsonl"))
    iso3166_countries_and_regions = load_jsonl(os.path.join(test_doi_path, "iso3166_countries_and_regions.jsonl"))
    grid_to_home_url = load_jsonl(os.path.join(test_doi_path, "grid_to_home_url.jsonl"))
    groupings = load_jsonl(os.path.join(test_doi_path, "groupings.jsonl"))
    mag_affiliation_override = load_jsonl(os.path.join(test_doi_path, "mag_affiliation_override.jsonl"))

    analysis_schema_path = schema_folder()
    with CliRunner().isolated_filesystem() as t:
        tables = [
            Table("crossref_events", False, dataset_id_all, crossref_events, "crossref_events", analysis_schema_path),
            Table(
                "crossref_metadata", True, dataset_id_all, crossref_metadata, "crossref_metadata", analysis_schema_path
            ),
            Table("crossref_fundref", True, dataset_id_all, crossref_fundref, "crossref_fundref", analysis_schema_path),
            Table("Affiliations", True, dataset_id_all, mag.affiliations, "MagAffiliations", analysis_schema_path),
            Table("FieldsOfStudy", True, dataset_id_all, mag.fields_of_study, "MagFieldsOfStudy", analysis_schema_path),
            Table(
                "PaperAuthorAffiliations",
                True,
                dataset_id_all,
                mag.paper_author_affiliations,
                "MagPaperAuthorAffiliations",
                analysis_schema_path,
            ),
            Table(
                "PaperFieldsOfStudy",
                True,
                dataset_id_all,
                mag.paper_fields_of_study,
                "MagPaperFieldsOfStudy",
                analysis_schema_path,
            ),
            Table("Papers", True, dataset_id_all, mag.papers, "MagPapers", analysis_schema_path),
            Table("open_citations", True, dataset_id_all, open_citations, "open_citations", analysis_schema_path),
            Table("unpaywall", True, dataset_id_all, unpaywall, "unpaywall", analysis_schema_path),
            Table("grid", True, dataset_id_all, grid, "grid", analysis_schema_path),
            Table(
                "iso3166_countries_and_regions",
                False,
                dataset_id_all,
                iso3166_countries_and_regions,
                "iso3166_countries_and_regions",
                analysis_schema_path,
            ),
            Table(
                "grid_to_home_url",
                False,
                dataset_id_settings,
                grid_to_home_url,
                "grid_to_home_url",
                analysis_schema_path,
            ),
            Table("groupings", False, dataset_id_settings, groupings, "groupings", analysis_schema_path),
            Table(
                "mag_affiliation_override",
                False,
                dataset_id_settings,
                mag_affiliation_override,
                "mag_affiliation_override",
                analysis_schema_path,
            ),
            Table(
                "PaperAbstractsInvertedIndex",
                True,
                dataset_id_all,
                [],
                "MagPaperAbstractsInvertedIndex",
                analysis_schema_path,
            ),
            Table("Journals", True, dataset_id_all, [], "MagJournals", analysis_schema_path),
            Table("ConferenceInstances", True, dataset_id_all, [], "MagConferenceInstances", analysis_schema_path),
            Table("ConferenceSeries", True, dataset_id_all, [], "MagConferenceSeries", analysis_schema_path),
            Table(
                "FieldOfStudyExtendedAttributes",
                True,
                dataset_id_all,
                [],
                "MagFieldOfStudyExtendedAttributes",
                analysis_schema_path,
            ),
            Table(
                "PaperExtendedAttributes", True, dataset_id_all, [], "MagPaperExtendedAttributes", analysis_schema_path
            ),
            Table("PaperResources", True, dataset_id_all, [], "MagPaperResources", analysis_schema_path),
            Table("PaperUrls", True, dataset_id_all, [], "MagPaperUrls", analysis_schema_path),
            Table("PaperMeSH", True, dataset_id_all, [], "MagPaperMeSH", analysis_schema_path),
            Table("orcid", False, dataset_id_all, [], "orcid", analysis_schema_path),
        ]

        bq_load_tables(tables=tables, bucket_name=bucket_name, release_date=release_date, data_location=data_location)


def aggregate_events(events: List[Event]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """Aggregate events by source into total events for all time, monthly and yearly counts.

    :param events: list of events.
    :return: list of events for each source aggregated by all time, months and years.
    """

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


def sort_events(events: List[Dict], months: List[Dict], years: List[Dict]):
    """Sort events in-place.

    :param events: events all time.
    :param months: events by month.
    :param years: events by year.
    :return: None.
    """

    events.sort(key=lambda x: x["source"])
    months.sort(key=lambda x: f"{x['month']}{x['source']}{x['count']}")
    years.sort(key=lambda x: f"{x['year']}{x['source']}{x['count']}")


def make_doi_table(dataset: ObservatoryDataset) -> List[Dict]:
    """Generate the DOI table from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

    records = []
    for paper in dataset.papers:
        # Doi, events and grids
        doi = paper.doi.upper()
        events = make_doi_events(doi, paper.events)
        grids = make_doi_grids(paper.authors)

        # Affiliations: institutions, countries, regions, subregion, funders, journals, publishers
        institutions = make_doi_institutions(paper.authors)
        countries = make_doi_countries(paper.authors)
        regions = make_doi_regions(paper.authors)
        subregions = make_doi_subregions(paper.authors)
        funders = make_doi_funders(paper.funders)
        journals = make_doi_journals(paper.journal)
        publishers = make_doi_publishers(paper.publisher)

        # Make final record
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
                    "authors": [],
                    "journals": journals,
                    "publishers": publishers,
                },
            }
        )

    # Sort to match with sorted results
    records.sort(key=lambda r: r["doi"])

    return records


def make_doi_events(doi: str, event_list: EventsList) -> Dict:
    """Make the events for a DOI table row.

    :param doi: the DOI.
    :param event_list: a list of events for the paper.
    :return: the events for the DOI table.
    """

    events_total, events_months, events_years = aggregate_events(event_list)

    # When no events, events is None
    events = None
    if len(events_total):
        events = {
            "doi": doi,
            "events": events_total,
            "months": events_months,
            "years": events_years,
        }

    return events


def make_doi_grids(author_list: AuthorList) -> List[str]:
    """Make the grid ids for a DOI table row.

    :param author_list: the list of authors for the paper.
    :return: the grid ids for the paper.
    """

    return list(set([author.institution.grid_id for author in author_list]))


def make_doi_funders(funder_list: FunderList) -> List[Dict]:
    """Make a DOI table row funders affiliation list.

    :param funder_list: the funders list.
    :return: the funders affiliation list.
    """

    # Funders
    funders = {}
    for funder in funder_list:
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

    return funders


def make_doi_journals(journal: Journal) -> List[Dict]:
    """Make the journal affiliation list for a DOI table row.

    :param journal: the paper's journal.
    :return: the journal affiliation list.
    """

    return [
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


def to_affiliations_list(dict_: Dict):
    """Convert affiliation dict into a list.

    :param dict_: affiliation dict.
    :return: affiliation list.
    """

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


def make_doi_publishers(publisher: Publisher) -> List[Dict]:
    """Make the publisher affiliations for a DOI table row.

    :param publisher: the paper's publisher.
    :return: the publisher affiliations list.
    """

    return [
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


def make_doi_institutions(author_list: AuthorList) -> List[Dict]:
    """Make the institution affiliations for a DOI table row.

    :param author_list: the paper's author list.
    :return: the institution affiliation list.
    """

    institutions = {}

    for author in author_list:
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

    return to_affiliations_list(institutions)


def make_doi_countries(author_list: AuthorList):
    """Make the countries affiliations for a DOI table row.

    :param author_list: the paper's author list.
    :return: the countries affiliation list.
    """

    countries = {}

    for author in author_list:
        inst = author.institution
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

    return to_affiliations_list(countries)


def make_doi_regions(author_list: AuthorList):
    """Make the regions affiliations for a DOI table row.

    :param author_list: the paper's author list.
    :return: the regions affiliation list.
    """

    regions = {}
    for author in author_list:
        inst = author.institution
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

    return to_affiliations_list(regions)


def make_doi_subregions(author_list: AuthorList):
    """Make the subregions affiliations for a DOI table row.

    :param author_list: the paper's author list.
    :return: the subregions affiliation list.
    """

    subregions = {}
    for author in author_list:
        inst = author.institution

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

    return to_affiliations_list(subregions)


def calc_percent(value: float, total: float) -> float:
    """Calculate a percentage and round to 2dp.

    :param value: the value.
    :param total: the total.
    :return: the percentage.
    """

    return round(value / total * 100, 2)


def make_country_table(dataset: ObservatoryDataset) -> List[Dict]:
    """Generate the Observatory Country table from an ObservatoryDataset instance.

    :param dataset: the Observatory Dataset.
    :return: table rows.
    """

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
                    "oa": {"total_outputs": oa, "percent": calc_percent(oa, total_outputs)},
                    "green": {"total_outputs": green, "percent": calc_percent(green, total_outputs)},
                    "gold": {"total_outputs": gold, "percent": calc_percent(gold, total_outputs)},
                    "gold_doaj": {"total_outputs": gold_doaj, "percent": calc_percent(gold_doaj, total_outputs)},
                    "hybrid": {"total_outputs": hybrid, "percent": calc_percent(hybrid, total_outputs)},
                    "bronze": {"total_outputs": bronze, "percent": calc_percent(bronze, total_outputs)},
                    "green_only": {"total_outputs": green_only, "percent": calc_percent(green_only, total_outputs)},
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

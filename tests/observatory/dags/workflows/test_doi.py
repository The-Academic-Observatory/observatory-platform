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

import os
import random
import unittest
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Dict
from typing import List

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.dummy_operator import DummyOperator
from faker import Faker

from observatory.dags.workflows.doi import DoiWorkflow
from observatory.platform.utils.airflow_utils import set_task_state
from observatory.platform.utils.test_utils import ObservatoryEnvironment, ObservatoryTestCase, module_file_path

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
    region: str = None
    subregion: str = None
    papers: List[Paper] = None


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
    event_date: pendulum.Date = None


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
    n_authors: int = 100,
    n_papers: int = 1000,
    n_fields_of_study_per_level: int = 200,
) -> ObservatoryDataset:
    faker = Faker()

    # Institutions
    inst_curtin = Institution(
        1,
        name="Curtin University",
        grid_id="grid.1032.0",
        country_code="AUS",
        region="Oceania",
        subregion="Australia and New Zealand",
    )
    inst_anu = Institution(
        2,
        name="Australia National University",
        grid_id="grid.1001.0",
        country_code="AUS",
        region="Oceania",
        subregion="Australia and New Zealand",
    )
    inst_akl = Institution(
        3,
        name="University of Auckland",
        grid_id="grid.9654.e",
        country_code="NZL",
        region="Oceania",
        subregion="Australia and New Zealand",
    )
    institutions = [inst_curtin, inst_anu, inst_akl]

    # Funders
    funders = []
    doi_prefix = 1000
    for i, _ in enumerate(range(n_funders)):
        funders.append(Funder(i, name=faker.company(), doi=make_doi(doi_prefix)))
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
            event_date_ = pendulum.date(year=event_date_.year, month=event_date_.month, day=event_date_.day)
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
        lookup_totals = dict()
        lookup_months = dict()
        lookup_years = dict()
        for event in paper.events:
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

        events_ = [{"source": source, "count": count} for source, count in lookup_totals.items()]
        months_ = [
            {"source": source, "month": month, "count": count} for (source, month), count in lookup_months.items()
        ]
        years_ = [{"source": source, "year": year, "count": count} for (source, year), count in lookup_years.items()]

        events.append({"doi": paper.doi, "events": events_, "months": months_, "years": years_})

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
            oa_location = {"host_type": "publisher", "license": paper.license}
            oa_locations.append(oa_location)

        if paper.is_in_institutional_repo:
            oa_location = {"host_type": "repository", "license": paper.license}
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
            {"name": funder.name, "doi": funder.doi,}
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
                "DOI": paper.doi,
                "is_referenced_by_count": len(paper.cited_by),
                "issued": {
                    "date_parts": [[paper.published_date.year, paper.published_date.month, paper.published_date.day]]
                },
                "funder": funders,
            }
        )

    return records


class TestDataGenerator(unittest.TestCase):
    def test_datagen(self):
        observatory_dataset = make_observatory_dataset()
        open_citations = make_open_citations(observatory_dataset)
        crossref_events = make_crossref_events(observatory_dataset)
        mag = make_mag(observatory_dataset)
        fundref = make_fundref(observatory_dataset)
        unpaywall = make_unpaywall(observatory_dataset)
        crossref_metadata = make_crossref_metadata(observatory_dataset)

        # Save to jsonl

        # Upload to google cloud storage
        # Load into BigQuery in a temporary dataset
        # Run DOI workflow as normal
        # Compute expected output
        # Check that expected outputs match

        a = 1


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

    def test_telescope(self):
        """Test the DOI telescope end to end.

        :return: None.
        """

        env = ObservatoryEnvironment(self.gcp_project_id, self.gcp_data_location)

        with env.create():
            # Make dag
            doi_dag = DoiWorkflow().make_dag()

            # Test that sensors do go into the 'up_for_reschedule' state as the DAGs that they wait for haven't run
            execution_date = pendulum.datetime(year=2020, month=11, day=1)
            expected_state = "up_for_reschedule"
            with env.create_dag_run(doi_dag, execution_date):
                for task_id in DoiWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f"{task_id}_sensor", doi_dag, execution_date=execution_date)
                    self.assertEqual(expected_state, ti.state)

            # Run Dummy Dags
            execution_date = pendulum.datetime(year=2020, month=11, day=2)
            expected_state = "success"
            for dag_id in DoiWorkflow.SENSOR_DAG_IDS:
                dag = make_dummy_dag(dag_id, execution_date)
                with env.create_dag_run(dag, execution_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    ti = env.run_task("dummy_task", dag, execution_date=execution_date)
                    self.assertEqual(expected_state, ti.state)

            # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
            with env.create_dag_run(doi_dag, execution_date):
                for task_id in DoiWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f"{task_id}_sensor", doi_dag, execution_date=execution_date)
                    self.assertEqual(expected_state, ti.state)

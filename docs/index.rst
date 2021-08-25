=======================
Academic Observatory Workflows
=======================
Academic Observatory Workflows provides Apache Airflow workflows for fetching, processing and analysing
data about academic institutions.

The workflows include: Crossref Events, Crossref Fundref, Crossref Metadata, Geonames, GRID, Microsoft Academic
Graph, Open Citations, ORCID, Scopus, Unpaywall and Web of Science.

Telescope Workflows
=======================
A telescope a type of workflow used to ingest data from different data sources, and to run workflows that process and
output data to other places. Workflows are built on top of Apache Airflow's DAGs.

.. toctree::
    :maxdepth: 1

    workflows/crossref_events
    workflows/crossref_fundref
    workflows/crossref_metadata
    workflows/doab
    workflows/geonames
    workflows/google_analytics
    workflows/google_books
    workflows/grid
    workflows/jstor
    workflows/mag
    workflows/oapen_metadata
    workflows/oapen_irus_uk
    workflows/orcid
    workflows/scopus
    workflows/terraform
    workflows/ucl_discovery
    workflows/unpaywall
    workflows/wos

Analytic Workflows
=======================
Analytic workflows process the data ingested by telescope workflows and are also built on top of Apache Airflow DAGs.

.. toctree::
    :maxdepth: 1

    workflows/doi

Misc
=======================
License and API reference details.

.. toctree::
    :maxdepth: 1

    license


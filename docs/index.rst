==============================
Academic Observatory Workflows
==============================
Academic Observatory Workflows provides Apache Airflow workflows for fetching, processing and analysing
data about academic institutions.

The workflows include: Crossref Events, Crossref Fundref, Crossref Metadata, Geonames, GRID, Microsoft Academic
Graph, Open Citations, ORCID, Scopus, Unpaywall and Web of Science.

Telescope Workflows
====================
A telescope a type of workflow used to ingest data from different data sources, and to run workflows that process and
output data to other places. Workflows are built on top of Apache Airflow's DAGs.

.. toctree::
    :maxdepth: 2

    telescopes/index

Analytic Workflows
===================
Analytic workflows process the data ingested by telescope workflows and are also built on top of Apache Airflow DAGs.

.. toctree::
    :maxdepth: 2

    workflows/index


License & Contributing Guidelines
=================================
Information about licenses, contributing guidelines etc.

.. toctree::
    :maxdepth: 1

    license

Python API Reference
=====================
Documentation for the academic-observatory-workflow Python API.

.. toctree::
    :maxdepth: 3

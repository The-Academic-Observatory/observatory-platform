Data quality profiling
----------------------------
A data profiling system consisting of analysers and analyser modules written in Python.  An analyser loads modules for profiling various aspects of a data set.

The current pipline consists of the following steps:

1. Execute a script that loads a data set analyser. The analyser loads the configured modules.

2. The modules query the stored data set for relevant characteristics.  Currently these are bigquery queries.  In the future, it may access the data directly as part of the ETL pipeline.

3. Profile metrics are computed.

4. Database records containing these metrics are constructed.  Currently this is elastic search documents.

5. The database records are saved.  Currenlty this means the new elastic search documents are indexed.

6. Kibana indices need to be manually created for each elastic search index.

7. Any relevant visualisations can now be manually created in Kibana.


Analyser architecture
=======================

.. toctree::
    :maxdepth: 1

    architecture

Analysers
==================================
.. toctree::
    :maxdepth: 4

    mag/index
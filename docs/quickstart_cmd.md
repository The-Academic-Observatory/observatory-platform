# Command Line Quick Start
The following is a quickstart tutorial to get you started with the Academic Observatory command line tool.

## Academic Observatory local development environment
To start the local Academic Observatory platform:
```bash
observatory platform start
```

Once the Academic Observatory has started, you can access the Apache Airflow UI at the following URL: 
[http://localhost:8080](http://localhost:8080).

To stop the local Academic Observatory platform:
```bash
observatory platform stop
```

Start the platform with a different Apache Airflow UI port:
```bash
observatory platform start --airflow-ui-port 8081
```

Note that if the --airflow-ui-port command is set, then the Airflow UI will be hosted on a different port,
for example, if you used port 8081 as in the above example then the URL would be 
[http://localhost:8081](http://localhost:8081).

Start the platform with a different Apache Airflow DAGS path:
```bash
observatory platform start --airflow-dags-path /your/path
```

Start the platform with a different Apache Airflow PostgreSQL data path:
```bash
observatory platform start --airflow-postgres-path /your/path
```

To get help with the Academic Observatory platform commands:
```bash
observatory platform --help
```

The default paths can be listed by typing the above help command, the defaults are typically:
* Apache Airflow DAGS folder: the `dags` module in the installed academic_observatory Python package.
* Apache Airflow PostgreSQL data folder: `~/.academic-observatory/mnt/airflow-postgres`.

The following environment variables need to be set to enable workflows to upload data to Google Cloud:
* GOOGLE_PROJECT_ID: the unique identifier for your Google Cloud project, see 
[here](https://cloud.google.com/resource-manager/docs/creating-managing-projects) for more details.
* GOOGLE_APPLICATION_CREDENTIALS: the path to the authentication credentials for the service account associated with 
your project, see [here](https://cloud.google.com/docs/authentication/getting-started) for more details.
* GOOGLE_BUCKET_NAME: the name of the Google Cloud Storage Bucket where the processed files will be saved to.

## GRID
To download all historical [Global Research Identifier Database (GRID)](https://grid.ac/) releases:
```bash
aoutil grid download
```

To get help with the above command, including what other parameters are available, run:
```bash
aoutil grid download -h
```

To create the GRID index:
```bash
aoutil grid index
```

To get help with the above command, including what other parameters are available, run:
```bash
aoutil grid index -h
```

## OAI-PMH
Create a CSV file that has a column of potential OAI-PMH URLs and a header:
```bash
url
http://digitalknowledge.cput.ac.za/oai/request
http://vital.seals.ac.za:8080/vital/oai/provider
http://dspace.nwu.ac.za/oai/request?verb=ListIdentifiers&metadataPrefix=oai_dc
http://eprints.ru.ac.za/cgi/oai2
http://uir.unisa.ac.za/oai/request
```

Supply this CSV to the following command, to fetch the given list of OAI-PMH endpoints:
```bash
aoutil oai_pmh fetch_endpoints --input oai_pmh_sources_small.csv --key url
```

To get help with this command type:
```bash
aoutil oai_pmh fetch_endpoints -h
```

After running the fetch_endpoints command, you can fetch OAI-PMH records within a given date range:
```bash
aoutil oai_pmh fetch_records --start_date 2019-11-01 --end_date 2019-11-10
```

To get help with the above command, including what other parameters are available, run:
```bash
aoutil oai_pmh fetch_records -h
```

## Common Crawl
To run the next command you will need:
* A table in `BigQuery` with the `Common Crawl` index and the `GRID` index joined and the institutes you would
like to get the full text data for in the table.
* The environment variable `GOOGLE_APPLICATION_CREDENTIALS` needs to be set. See the Google Cloud
[Getting Started with Authentication](https://cloud.google.com/docs/authentication/getting-started) guide for more 
details.

To fetch the full text for a particular University, run the following, this example is for Curtin University.
```bash
aoutil common_crawl fetch_full_text --table_name coki-jamie-dev.grid_common_crawl.curtin_demo --start_date 2019-08 \
    --end_date 2019-09
```

To get help with the above command, including what other parameters are available, run:
```bash
aoutil common_crawl fetch_full_text -h
```
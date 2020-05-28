# Command Line Quick Start
The following is a quickstart tutorial to get you started with the Academic Observatory command line tool.

## Dependencies
Follow these instructions to setup the Academic Observatory dependencies.

### Docker
Make sure that Docker Engine or Docker Desktop are installed:
* [Install Docker Engine](https://docs.docker.com/engine/install/): for Ubuntu.
* [Install Docker Desktop](https://docs.docker.com/desktop/): for Mac or Windows.

### Fernet key
Create a Fernet key and set the FERNET_KEY environment variable in ~/.bashrc:
```bash
echo export FERNET_KEY=`observatory generate fernet-key` >> ~/.bashrc && source ~/.bashrc
```

### Google account authentication 
Set the GOOGLE_APPLICATION_CREDENTIALS environment variable:
* See [Getting Started with Authentication](https://cloud.google.com/docs/authentication/getting-started) for 
instructions on how to create a service account key and how to set the environment variable. 

You can add the GOOGLE_APPLICATION_CREDENTIALS environment variable to `~/.bashrc` if you don't want to keep entering
it.

### Config file
Generate a config.yaml file:
```bash
observatory generate config.yaml
```

You should see the following output:
```bash
Generating config.yaml...
config.yaml saved to: "/home/user/.observatory/config.yaml"
Please customise the following parameters in config.yaml:
  - project_id
  - bucket_name
```

Customise the project_id and bucket_name fields:
* project_id: the unique identifier for the Google Cloud project that should be used with the Academic Observatory. See 
[here](https://cloud.google.com/resource-manager/docs/creating-managing-projects) for more details.
* bucket_name: the name of the Google Cloud Storage Bucket where the processed files will be saved to.

## Local Development Platform
To start the local Academic Observatory platform:
```bash
observatory platform start
```

You should see the following output:
```bash
Academic Observatory: all dependencies found                                    
...
Academic Observatory: built                                                     
Academic Observatory: started                                                   
View the Apache Airflow UI at http://localhost:8080
```

Once the Academic Observatory has started, you can access the Apache Airflow UI at the following URL: 
[http://localhost:8080](http://localhost:8080).

To stop the local Academic Observatory platform:
```bash
observatory platform stop
```

You should see the following output:
```bash
Academic Observatory: all dependencies found                                    
...
Academic Observatory: stopped                                                   
```

Host the Apache Airflow UI port on a different port, e.g. port 8081:
```bash
observatory platform start --airflow-ui-port 8081
```

You should see the following output:
```bash
Academic Observatory: all dependencies found                                    
...
Academic Observatory: built                                                     
Academic Observatory: started                                                   
View the Apache Airflow UI at http://localhost:8081 
```

Start the platform with a different Apache Airflow DAGs path:
```bash
observatory platform start --dags-path /your/path
```

Start the platform with a different Apache Airflow PostgreSQL data path:
```bash
observatory platform start --airflow-postgres-path /your/path
```

To get help with the Academic Observatory platform commands, including the default DAGs and data paths:
```bash
observatory platform --help
```

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
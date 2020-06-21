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

## Github action
Create "google_credentials" secret in this github repository that contains a json key for the service account that can modify buckets. 
The credentials are used to sign in with gcloud. The content of this repository is then uploaded to a 'dev' or 'prod' bucket.
The github action will look for a bucket that contains the given pattern and ends with 'dev' or 'prod'. 
 
The pattern that can be used with the terraform setup is 'github-'

## Terraform / cloud composer platform
### First time setting-up terraform workspaces
This only has to be done for the 'coki' organisation once. If someone already completed these steps,
there is no need to do them as well.

You can check if the workspaces are already set-up by going to the 'coki' organisation at https://app.terraform.io
and see if there are any workspaces listed.

To set-up workspaces:
- Create/get personal acccess github token that is used for github clone.
- Create/get google credentials file  
Sign in to terraform cloud at https://app.terraform.io and make sure you have access to the 'coki' organisation.
- Create terraform personal user token and save this to a file.

Build the docker image:
```bash
observatory terraform build --terraform_token_file=<token.txt>
```
Initialise the workspaces:  
```
observatory terraform setup_workspaces --google_credentials_file=<file.json> --terraform_token_file=<token.txt> --github_token=<token_string>
```

###General usage:  
#### First time set-up
Sign in to terraform cloud at https://app.terraform.io and make sure you have access to the 'coki' organisation.
- Create terraform personal user token and save this to a file: https://app.terraform.io/app/settings/tokens?source=terraform-login  
- Build the docker image:  
```
observatory terraform build --terraform_token_file=<token.txt>
```
  
#### Controlling composer environments
To turn composer environments on or off:  
```bash
observatory terraform ao-dev --on/off --plan/apply --terraform_token_file=<token.txt>
observatory terraform ao-prod --on/off --plan/apply --terraform_token_file=<token.txt>
```
Use `--on` or `--off` to turn on/off the respective composer environment.  
Use `--plan` to see which resources terraform will create/update/destroy.  
Use `--apply` to create/update/destroy resources.  

Note that it can sometimes take up to 30 minutes to create the composer environment.

#### Changing variables
Variables such as machine-instance and disk-size can be changed inside the composer workspaces at https://app.terraform.io/.  
After changing any variables, re-run `observatory terraform` for the respective environment with `--on` and `--apply`.

#### Workspace locking
The state of a workspace can be manually locked at https://app.terraform.io/app/COKI-project/workspaces/composer_dev/settings/lock.  
This can be useful for example if you want to prevent others from turning off the composer environment while you're using it.  
Please don't forget to unlock it when you are finished.

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
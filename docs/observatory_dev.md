# Observatory Platform Development Environment
The following is a tutorial for running the local Observatory Platform development environment.

Make sure that you have followed the installation instructions to install the Observatory Platform on your system.

## Prepare configuration files
Generate a config.yaml file:
```bash
observatory generate config.yaml
```

You should see the following output:
```bash
The file "/home/user/.observatory/config.yaml" exists, do you want to overwrite it? [y/N]: y
config.yaml saved to: "/home/user/.observatory/config.yaml"
Please customise the following parameters in config.yaml:
  - project_id
  - data_location
  - download_bucket_name
  - transform_bucket_name
  - google_application_credentials
```

The following parameters in the generated config.yaml need to be customised:
* project_id: a Google Cloud project id. See 
[Creating and managing projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects) for more 
details.
* data_location: the location where BigQuery data will be stored (same as the bucket location).
* download_bucket_name: the name of the Google Cloud Storage bucket where downloaded data will be stored.
* transform_bucket_name: the name of the Google Cloud Storage bucket where transformed data will be stored.
* google_application_credentials: the path to the JSON key for a Google Cloud service account that has permissions
to access the two buckets and the roles `roles/bigquery.admin` and `roles/storagetransfer.admin`.
See [Getting Started with Authentication](https://cloud.google.com/docs/authentication/getting-started) for 
instructions on how to create a service account key.

## Running the local development environment
See below for instructions on how to start the Observatory Platform, view Observatory Platform UIs, stop the 
Observatory Platform and customise other settings. 

### Start the Observatory Platform
To start the local Observatory Platform development environment:
```bash
observatory platform start
```

You should see the following output:
```bash
Observatory Platform: all dependencies found                                    
  Docker:
   - path: /usr/bin/docker
   - running
  Host machine settings:
   - observatory home: /home/user/.observatory
   - data-path: /home/user/.observatory/data
   - dags-path: /home/user/workspace/observatory-platform/observatory_platform/dags
   - logs-path: /home/user/.observatory/logs
   - postgres-path: /home/user/.observatory/postgres
   - host-uid: 1000
  Docker Compose:
   - path: /home/user/workspace/observatory-platform/venv/bin/docker-compose
  config.yaml:
   - path: /home/user/.observatory/config.yaml
   - file valid
Observatory Platform: built                                                     
Observatory Platform: started                                                   
View the Apache Airflow UI at http://localhost:8080
```

### Viewing the Apache Airflow and Flower UIs
Once the Observatory Platform has started, the following UIs can be accessed:
* Apache Airflow UI at [http://localhost:8080](http://localhost:8080)
* Flower UI at [http://localhost:5555](http://localhost:5555)

### Stop the Observatory Platform
To stop the Observatory Platform:
```bash
observatory platform stop
```

You should see the following output:
```bash
Observatory Platform: all dependencies found                                    
...
Observatory Platform: stopped                                                   
```

### Specify a config.yaml file
To specify a different config.yaml file use the `--config-path` parameter when starting the Observatory Platform:
```bash
observatory platform start --config-path /your/path/to/config.yaml
```

### Specify a DAGs folder
To specify a different dags folder use the `--dags-path` parameter when starting the Observatory Platform:
```bash
observatory platform start --dags-path /your/path/to/dags
```

### Specify a data folder
To specify a different folder to mount as the host machine's data folder, use the `--data-path` parameter when 
starting the Observatory Platform:
```bash
observatory platform start --data-path /your/path/to/data
```

### Specify a logs folder
To specify a different folder to mount as the host machine's logs folder, use the `--logs-path` parameter when 
starting the Observatory Platform:
```bash
observatory platform start --logs-path /your/path/to/logs
```

### Specify a PostgreSQL folder
To specify a different folder to mount as the host machine's PostgreSQL data folder, use the `--postgres-path` parameter 
when starting the Observatory Platform:
```bash
observatory platform start --postgres-path /your/path/to/postgres
```

### Specify a user id
To specify different user id, which is used to set the ownership of the volume mounts, use the following command
when starting the Observatory Platform:
```bash
observatory platform start --host-uid 5000
```

### Specify a group id
To specify different group id, which is used to set the ownership of the volume mounts, use the following command
when starting the Observatory Platform:
```bash
observatory platform start --host-gid 5000
```

### Getting help
To get help with the Observatory Platform commands:
```bash
observatory --help
```

To get help with the Observatory Platform platform command:
```bash
observatory platform --help
```

To get help with the Observatory Platform generate command:
```bash
observatory generate --help
```

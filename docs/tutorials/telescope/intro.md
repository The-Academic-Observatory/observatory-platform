# Background
## Description of a telescope
The observatory platform collects data from many different sources. Each individual data source in the observatory is
 referred to as a telescope.  
The telescope can be seen as a workflow or data pipeline and should try to capture the data in its original state as
 much as possible.  
The general workflow can be described with these tasks:
 - Extract the raw data from an external source
 - Store the raw data in a bucket
 - Transform the data, so it is ready to be loaded into the data warehouse
 - Store the transformed data in a bucket
 - Load the data into the data warehouse

## Managing telescopes with Airflow
The telescopes are all managed using Airflow. 
This workflow management system helps to schedule and monitor the many different telescopes.  
Airflow works with DAG (Directed Acyclic Graph) objects that are defined in a Python script. 
The definition of a DAG according to Airflow is as follows:
 > A dag (directed acyclic graph) is a collection of tasks with directional dependencies. A dag also has a schedule, a start date and an end date (optional). For each schedule, (say daily or hourly), the DAG needs to run each individual tasks as their dependencies are met.

Generally speaking, one DAG maps to one telescope.

## Telescopes and Google Cloud Platform
The Observatory Platform currently uses Google Cloud Platform as a platform for data storage and a data warehouse.  
This means that the data is stored in Google Cloud Storage buckets and loaded into Google Cloud BigQuery, which
 functions as the data warehouse.  
To be able to access the different Google Cloud resources (such as Storage Buckets and BigQuery), the
 GOOGLE_APPLICATION_CREDENTIALS environment variable is set on the Compute Engine that hosts Airflow.
This is all done when installing either the Observatory Platform Development Environment or Terraform Environment.  
For any Google Cloud utility functions that are used in a telescope, it is assumed that these default
 credentials are set.  

## The telescope templates
Initially the telescopes in the observatory platform were each developed individually.  
There would be a telescope and release class that was unique for each telescope.  
After developing a few telescopes it became clear that there are many similarities between the telescopes
 and the classes that were developed.  
For example, many tasks such as uploading data to a bucket or loading data into BigQuery were the same for different
 telescopes and only variables like filenames and schemas would be different.  
The same properties were also often implemented, for example a download folder, release date and the many Airflow
 related properties such as the DAG id, schedule interval, start date etc.
 
These similarities prompted the development of a telescope template that can be used as a basis for a new telescope.  
The template abstracts away the code to create the DAG object used in Airflow, making it possible to use the template
 without previous Airflow knowledge, although having basic Airflow knowledge might help to understand the
  possibilities and limitations of the template.  
The template also implements properties that are often used and common tasks such as cleaning up local files at the
 end of the telescope.  
The base template is used for three other templates that implement more specific tasks for loading data into
 BigQuery and have some properties set to specific values (such as whether previous DAG runs should be run using the
  airflow 'catchup' setting).  
The base template and the other three templates (snapshot, stream and organisation) are all explained in more detail
 below.  
Each of the templates also have their own corresponding release class, this class contains properties and methods
 that are related to the specific release of a data source.  
# Elastic Import

```eval_rst
+------------------------------+---------+
| Summary                      |         |
+==============================+=========+
| Runs on remote worker        | False   |
+------------------------------+---------+
| Catchup missed runs          | False   |
+------------------------------+---------+
| Credentials Required         | Yes     |
+------------------------------+---------+
| Uses Telescope Template      | Workflow|
+------------------------------+---------+
```

## Authentication
This workflow connects to both the Elasticsearch and Kibana server.  
Through the Elasticsearch client old indices are deleted and new indices as well as aliases are created. 
Through the Kibana API new index patterns are created in each of the given kibana spaces.  
Both servers are accessed using the same API key, see below for info on how to create this.

### Required permissions
It is recommended to create a separate user account for this workflow through the Kibana UI, and to assign a role
 with a limited set of permissions to this account.
The minimal permissions that are required for this workflow are:

Elasticsearch  
- To create new indices  
  Indices: '*'  
  Privileges: 'create_index'
- To delete indices and add/remove aliases, for a limited set of indices  
  Indices: 'custom-*'
  Privileges: 'all'
  
Kibana  
- To create/delete index patterns, for a limited set of kibana spaces  
  Spaces: <required spaces>  
  Privileges: Customize -> Management -> Index Pattern Management: All

### Creating an API key
After setting up the permissions and creating a dedicated user account, it is possible to create an API key for this
 user through the Elasticsearch API. 
Creating an API key for a user other than yourself does require the cluster privilege 'grant_api_key' on your
 Elasticsearch user account.
 
For example to create an API key for the new user account named 'airflow', with the password 'my_password':
```
POST /_security/api_key/grant
{
  "grant_type": "password",
  "username" : "airflow",
  "password" : "my_password",
  "api_key" : {
    "name": "elastic_import_workflow"
  }
}
```

This will return a response with the API key id, name and api_key, for example:
```
{
  "id" : "OmyIQn4BWGb2uJ7Wu4uX",
  "name" : "elastic_import_workflow",
  "api_key" : "nNfS6FiGQdWJFGSo-4ACGg"
}
```

## Airflow connections
Note that all values need to be urlencoded. 
In the DAG file for this workflow are ElasticImportConfig instances defined.
In each config instance, there is a 'elastic_conn_key' and 'kibana_conn_key' variable defined.
These refer to the Airflow connections that are used to connect to the Elasticsearch/Kibana servers.

For example, if in one of the configs elastic_conn_key="elastic_main" and kibana_conn_key="kibana_main":

### elastic_main
The elastic_main Airflow connection is used to connect to the Elasticsearch server.  
To get the api_key_id and api_key, see the section above at 'Creating an API key'.
The Elasticsearch address and port can be found from the 'endpoint' info at the elastic.co portal. 

```yaml
elastic_main: https://<api_key_id>:<api_key>@<elastic_address>:<port>
```

For example if the api_key_id=OmyIQn4BWGb2uJ7Wu4uX, api_key=nNfS6FiGQdWJFGSo-4ACGg and the endpoint info shows 
 'https://gcp-project.es.us-west1.gcp.cloud.es.io:9243'

```yaml
elastic_main: https://OmyIQn4BWGb2uJ7Wu4uX:nNfS6FiGQdWJFGSo-4ACGg@gcp-project.es.us-west1.gcp.cloud.es.io:9243
```
 
### kibana_main
The kibana_main Airflow connection is used to connect to the Kibana server.

This connection is similar to the elastic Airflow connection, the only difference is that the kibana_address is used
 instead of the elastic_address. 
The api_key_id and api_key are likely to be the same.

```yaml
kibana_main: https://<api_key_id>:<api_key>@<kibana_address>:<port>
```

For example if the api_key_id=OmyIQn4BWGb2uJ7Wu4uX, api_key=nNfS6FiGQdWJFGSo-4ACGg and the endpoint info shows 
 'https://gcp-project.kb.us-west1.gcp.cloud.es.io:9243'

```yaml
kibana_main: https://OmyIQn4BWGb2uJ7Wu4uX:nNfS6FiGQdWJFGSo-4ACGg@gcp-project.kb.us-west1.gcp.cloud.es.io:9243
```

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
This workflow connects to both the Elasticsearch and Kibana server. Through the Elasticsearch client old indices are 
deleted and new indices as well as aliases are created. Through the Kibana API new index patterns are created in each 
of the given Kibana spaces. Both servers are accessed using the same API key, see below for info on how to create this.

### Creating the API key
In the Kibana dev console, run the below command, being careful to customise the following:
* index.names: add wildcard prefixes for indexes that this API key should have access to, or give it access
  to all indexes with the wildcard "*".
* applications.resources: add the specific spaces that the API key should have access to in the form "space:my-space-id",
  or give the API key access to all spaces with the wildcard "*".

```
POST /_security/api_key 
{
  "name": "airflow-api-key",
  "role_descriptors": {
    "role-airflow": {
      "cluster": [],
      "index": [
        {
          "names": [
            "my-index-prefix-a-*", "my-index-prefix-b-*"
          ],
          "privileges": [
            "read",
            "write",
            "create_index",
            "delete_index",
            "view_index_metadata",
            "manage"
          ]
        }
      ],
      "applications": [
        {
          "application": "kibana-.kibana",
          "privileges": [
            "feature_indexPatterns.all"
          ],
          "resources": [
            "space:my-space-id"
          ]
        }
      ]
    }
  }
}
```

This will return a response with the API key id, name and api_key, for example:
```
{
  "id" : "OmyIQn4BWGb2uJ7Wu4uX",
  "name" : "airflow-api-key",
  "api_key" : "nNfS6FiGQdWJFGSo-4ACGg"
}
```

## Airflow connections
Note that all values need to be urlencoded. In the DAG file for this workflow are ElasticImportConfig instances defined.
In each config instance, there is a 'elastic_conn_key' and 'kibana_conn_key' variable defined. These refer to the 
Airflow connections that are used to connect to the Elasticsearch/Kibana servers.

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

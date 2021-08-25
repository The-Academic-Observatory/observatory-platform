# Crossref Metadata

Crossref is a non-for-profit membership organisation working on making scholarly communications better. 
It is an official Digital Object Identifier (DOI) Registration Agency of the International DOI Foundation. 
They provide metadata for every DOI that is registered with Crossref.

Crossref Members send Crossref scholarly metadata on research which is collated and 
standardised into the Crossref metadata dataset. This dataset is made available through 
services and tools for manuscript tracking, searching, bibliographic management, 
library systems, author profiling, specialist subject databases, scholarly sharing networks
. _- source: [Crossref Metadata](https://www.crossref.org/services/metadata-retrieval/)_ 
and [schema details](https://github.com/Crossref/rest-api-doc/blob/master/api_format.md)

The corresponding table created in BigQuery is `crossref.crossref_metadataYYYYMMDD`.

```eval_rst
+------------------------------+---------+
| Summary                      |         |
+==============================+=========+
| Average runtime              | ? min   |
+------------------------------+---------+
| Average download size        | ? MB    |
+------------------------------+---------+
| Harvest Type                 | API     |
+------------------------------+---------+
| Harvest Frequency            | Monthly |
+------------------------------+---------+
| Runs on remote worker        | True    |
+------------------------------+---------+
| Catchup missed runs          | True    |
+------------------------------+---------+
| Table Write Disposition      | Truncate|
+------------------------------+---------+
| Update Frequency             | Monthly |
+------------------------------+---------+
| Credentials Required         | No      |
+------------------------------+---------+
| Uses Telescope Template      | Snapshot|
+------------------------------+---------+
| Each shard includes all data | Yes     |
+------------------------------+---------+
```

## Airflow connections
Note that all values need to be urlencoded.
In the config.yaml file, the following airflow connections are required:  

### crossref
This contains the Crossref Metadata Plus token (https://www.crossref.org/documentation/metadata-plus/).  
```yaml
crossref: http://:<token>@
```


## Latest schema
``` eval_rst
.. csv-table::
   :file: ../schemas/crossref_metadata_latest.csv
   :width: 100%
   :header-rows: 1
```
# Crossref Fundref

The Crossref Funder Registry is an open registry of grant-giving organization names and identifiers, which you use to find funder IDs and include them as part of your metadata deposits. 
It is a freely-downloadable RDF file. It is CC0-licensed and available to integrate with your own systems.  
Funder names from acknowledgements should be matched with the corresponding unique funder ID from the Funder Registry. See the [Funder Registry](https://www.crossref.org/services/funder-registry/) and [data details](https://github.com/CrossRef/rest-api-doc) for more information.

The corresponding table created in BigQuery is `crossref.crossref_fundrefYYYYMMDD`.

```eval_rst
+------------------------------+---------+
| Summary                      |         |
+==============================+=========+
| Average runtime              | 5 min   |
+------------------------------+---------+
| Average download size        | 50 MB   |
+------------------------------+---------+
| Harvest Type                 | API     |
+------------------------------+---------+
| Harvest Frequency            | Weekly  |
+------------------------------+---------+
| Runs on remote worker        | True    |
+------------------------------+---------+
| Catchup missed runs          | True    |
+------------------------------+---------+
| Table Write Disposition      | Truncate|
+------------------------------+---------+
| Update Frequency             | Random  |
+------------------------------+---------+
| Credentials Required         | No      |
+------------------------------+---------+
| Uses Telescope Template      | Snapshot|
+------------------------------+---------+
| Each shard includes all data | Yes     |
+------------------------------+---------+
```

## Latest schema
``` eval_rst
.. csv-table::
   :file: ../schemas/crossref_fundref_latest.csv
   :width: 100%
   :header-rows: 1
```
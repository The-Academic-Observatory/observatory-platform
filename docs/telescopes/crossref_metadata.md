# Crossref Metadata

Crossref is a non-for-profit membership organisation working on making scholarly communications better. 
It is an official Digital Object Identifier (DOI) Registration Agency of the International DOI Foundation. 
They provide metadata for every DOI that is registered with Crossref.

The corresponding table created in BigQuery is `crossref.crossref_metadataYYYYMMDD`.

```eval_rst
+------------------------------+---------+
| Summary                      |         |
+==============================+=========+
| Average runtime              | ? min   |
+------------------------------+---------+
| Average download size        | ? MB   |
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

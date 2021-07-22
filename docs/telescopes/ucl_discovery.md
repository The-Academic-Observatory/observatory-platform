# UCL Discovery

UCL Discovery is UCL's open access repository, showcasing and providing access to the full texts of UCL research publications.

The metadata for all eprints is obtained from the publicly available CSV file (https://discovery.ucl.ac.uk/cgi/search/advanced). 
Additionally for each eprint the total downloads and downloads per country is gathered from the publicly available stats
 (https://discovery.ucl.ac.uk/cgi/stats/report).


```eval_rst
+------------------------------+---------+
| Summary                      |         |
+==============================+=========+
| Average runtime              | ? min   |
+------------------------------+---------+
| Average download size        |  ? MB   |
+------------------------------+---------+
| Harvest Type                 |  API    |
+------------------------------+---------+
| Harvest Frequency            | Monthly |
+------------------------------+---------+
| Runs on remote worker        | True    |
+------------------------------+---------+
| Catchup missed runs          | True    |
+------------------------------+---------+
| Table Write Disposition      | Truncate|
+------------------------------+---------+
| Update Frequency             | Daily   |
+------------------------------+---------+
| Credentials Required         | No      |
+------------------------------+---------+
| Uses Telescope Template      | Snapshot|
+------------------------------+---------+
| Each shard includes all data | No      |
+------------------------------+---------+
```

## Latest schema
``` eval_rst
.. csv-table::
   :file: ../schemas/ucl_discovery_latest.csv
   :width: 100%
   :header-rows: 1
```
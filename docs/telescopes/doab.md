# Directory of Open Access Books
The Directory of Open Access Books (DOAB) is a directory of open-access peer reviewed scholarly books. 
Its aim is to increase discoverability of books. Currently, there are two requirements to take part in DOAB:
* Academic books in DOAB shall be available under an Open Access license (such as a Creative Commons license)
* Academic books in DOAB shall be subjected to independent and external peer review prior to publication
Data is downloaded from a csv file.

```eval_rst
+--------------------------+---------+
| Summary                  |         |
+==========================+=========+
| Average runtime          | 15m     |
+--------------------------+---------+
| Average download size    | 50MB    |
+--------------------------+---------+
| Harvest Type             | API     |
+--------------------------+---------+
| Harvest Frequency        | Weekly  |
+--------------------------+---------+
| Runs on remote worker    | False   |
+--------------------------+---------+
| Catchup missed runs      | False   |
+--------------------------+---------+
| Table Write Disposition  | Append  |
+--------------------------+---------+
| Update Frequency         | Daily   |
+--------------------------+---------+
| Credentials Required     | No      |
+--------------------------+---------+
```

## Schedule
The csv is updated daily and this telescope is scheduled to harvest the data weekly. 

## Results
There are two tables containing data related to this telescope:
  * The main table which contains all the up-to-date DOAB data. 
  * A partitioned table, where each partition contains the data of one run. The table is partitioned by the ingestion time.

If there are any edited or deleted entries, the old versions of these entries will first be deleted from the main table 
(using a MERGE statement) after which the updated versions are appended to this table.  

## Latest schema
``` eval_rst
.. csv-table::
   :file: ../schemas/doab_latest.csv
   :width: 100%
   :header-rows: 1
```
# Directory of Open Access Books
The Directory of Open Access Books (DOAB) is a directory of open-access peer reviewed scholarly books. 
Its aim is to increase discoverability of books. Currently, there are two requirements to take part in DOAB:
* Academic books in DOAB shall be available under an Open Access license (such as a Creative Commons license)
* Academic books in DOAB shall be subjected to independent and external peer review prior to publication
Data is downloaded both using a OAI-PMH harvester and from a csv file.

| Summary                 |        |
|-------------------------|--------|
| Average runtime         |   15m  |
| Average download size   |  50MB  |
| Harvest Type            |   API  |
| Harvest Frequency       | Weekly |
| Runs on remote worker   |  False |
| Catchup missed runs     |  False |
| Table Write Disposition | Append |
| Update Frequency        |  Daily |
| Credentials Required    |   No   |

## Schedule
The OAI-PMH and csv are both updated daily and this telescope is scheduled to harvest the data weekly. 
The earliest date for entries from the OAI-PMH harvester is 14-05-2018, while the earliest date for entries in the csv file is 04-11-2015.
It seems that the csv file is parsed for the OAI-PMH harvester, and this started on 14-05-2018.

Entries are obtained using the OAI-PMH harvester and then connected to the csv, so the start date of this telescope is set to 14-05-2018.

## Results
There are two tables containing data related to this telescope:
  * The main table which contains all the up-to-date DOAB data. 
  * A partitioned table, where each partition contains the data of one run. The table is partitioned by the ingestion time.

If there are any edited or deleted entries, the old versions of these entries will first be deleted from the main table 
(using a MERGE statement) after which the updated versions are appended to this table.  

## Tasks

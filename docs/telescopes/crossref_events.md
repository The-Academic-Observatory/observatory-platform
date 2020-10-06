# Crossref Events
Crossref Events collects Event data regarding scholarly content from multiple sources.  
Each Event represents a single observation. It contains a subject-relation-object triple and also has other supporting 
fields and metadata that indicate how, why, where, and by whom the Event was created.
See the [Event Data User Guide](https://www.eventdata.crossref.org/guide/) for more information. 

| Summary                 |        |
|-------------------------|--------|
| Average runtime         |   TBA  |
| Average download size   |   TBA  |
| Harvest Type            |   API  |
| Harvest Frequency       | Weekly |
| Runs on remote worker   |  True  |
| Catchup missed runs     |  False |
| Table Write Disposition | Append |
| Update Frequency        |  Daily |
| Credentials Required    |   No   |

## Schedule
The event data itself is updated daily and this telescope is scheduled to harvest the event data weekly. The earliest 
event data is from 17-02-2017, so the start date of this telescope is set to this date.

The first time this telescope is run it will download all event data up until the start date of this run, minus 1 day.
There is no need to download edited or deleted events at this time, since this is already updated in the original event data.

From the first release onwards it will download all new, edited and deleted events since the start date of the previous 
successful run up until the start date of this run, minus 1 day. 

This telescope will not catchup a backfill of possible missed runs, because it is more efficient to collect all new events 
(since the last run) at once, instead of splitting this up in multiple separate runs that will run consecutively.

## Results
There are two tables containing data related to this telescope:
  * The main table which contains all the up-to-date event data. 
  * A sharded table, where each shard contains the event data of one run. The table is automatically sharded by the date 
  at the end of the table name (crossref_eventsYYYYMMDD). Each shard contains data from a time period, not a single date, 
  the date in the table name refers to the end date of this time period.

If there are any edited or deleted events, the old versions of these events will first be deleted from the main table 
(using a MERGE statement) after which the updated versions are appended to this table.  
Note that edited/deleted events are still recorded in the table, only the data in relevant fields is updated or deleted.

## Tasks
### check_dependencies
Like all DAGs, first it will check if all required dependencies are met. These dependencies include the airflow variables 
'data_path', 'project_id', 'data_location', 'download_bucket_name' and 'transform_bucket_name'.

### download
This is where the event data is downloaded. If this the first time the DAG is run, only 'events' will be downloaded, no 
'edited' or 'deleted' events, since this is already updated in the events data. Each of these three categories has their 
own url and is processed separately.  

Each API request returns 10.000 events as well as a cursor that can be used to paginate through the results. For each of 
the three categories a different event file is made and events are appended to this file after each request. If a request 
fails due to too many retry errors, the current cursor will be stored in a file and when retrying the DAG/task the download 
will continue with this cursor.

The server regularly returns a 'server overloaded' error, so the number of retries is set relatively high to 15.

There are two download modes, 'sequential' or 'parallel'.  
With the sequential mode, the events for all 3 categories between the start and end date of this release are downloaded using a single thread.   
With the parallel mode, the time period between the start and end date is split up in multiple batches, and each batch 
will be processed on a different thread. The number of batches is equal to the number of threads available. 
 
For edited and deleted events it is currently not possible to specify an end date, therefore these can't be split up into 
batches in parallel mode. Therefore, all edited/deleted events between the start and end date of this run will be downloaded on the 
first thread. Hopefully this can be optimised in the future, once the end date can be used. See [gitlab](https://gitlab.com/crossref/issues/-/issues/877) 
for a status on this issue.

Once all events for the different batches and categories are downloaded they are combined into a single json file. If it 
turns out that there were no new/edited/deleted events in this release at all, the DAG will not continue.

### upload_downloaded
The json file with all the downloaded events combined is uploaded to a Google Cloud storage bucket.

### transform
Transforms the downloaded crossref events.  
The download file contains multiple lists, one for each request, and each list contains multiple events. Each event is 
transformed so that the field names are valid ('-' is replaced with '_') and have a valid timestamp at the 'occurred_at' field.  
The events are written out individually and separated by a newline, so the file is in a jsonl format that can be accepted 
by BigQuery.  

### upload_transformed
The jsonl file with all the transformed events is uploaded to a Google Cloud storage bucket.

### bq_load_shard
Create a table shard containing only events (incl. edited/deleted) of this release. The date in the table name is based on the end
date of this release.

### bq_delete_old
Run a BigQuery MERGE query, merging the sharded table of this release into the main table containing all
events.  
The tables are matched on the 'id' field and if a match occurs, a check will be done to determine whether the
'updated date' of the corresponding event in the main table either does not exist or is older than that of the
event in the sharded table. If this is the case, the event will be deleted from the main table.

### bq_append_new
All events from this release in the corresponding table shard will be appended to the main table.

### cleanup
Delete subdirectories for downloaded and transformed events files.
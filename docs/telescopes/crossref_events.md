# Crossref Events

> When someone links their data online, or mentions research on a social media site, we capture that event and make it
 available for anyone to use in their own way. We provide the unprocessed data—you decide how to use it.  

Before the expansion of the Internet, most discussion about scholarly content stayed within scholarly content, 
with articles citing each other. 
With the growth of online platforms for discussion, publication and social media, 
we have seen discussions extend into new, non-traditional venues. 
Crossref Event Data captures this activity and acts as a hub for the storage and distribution of this data. 
An event may be a citation in a dataset or patent, a mention in a news article, Wikipedia page or on a blog, 
or discussion and comment on social media.

When someone links their data online, or mentions research on, for example, Twitter, 
Wikipedia, or Reddit, Crossref’s uses a set of APIs to captures and records those events in 
their ‘Event dataset’. Events are tracked via their DOI and URLs, which enables Crossref to 
monitor where it’s been shared, linked, bookmarked, referenced or commented on. 
Crossref Event Data currently contains events from a range of data sources, including 
Crossref Metadata, DataCite Metadata, F1000Prime (Recommendations of research publications, 
Hypothes.is, The Lens (Cambia), Newsfeed, Reddit, Reddit Links, Stack Exchange Network, 
Twitter, Wikipedia, and Wordpress.com

See the crossref events [page](https://www.crossref.org/services/event-data/), and [data details](https://www.eventdata.crossref.org/guide/data/events/), for more information.

The corresponding table created in BigQuery are `crossref.crossref_events` and `crossref.crossref_events_partitions`.

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
| Harvest Frequency            | Weekly  |
+------------------------------+---------+
| Runs on remote worker        | True    |
+------------------------------+---------+
| Catchup missed runs          | False   |
+------------------------------+---------+
| Table Write Disposition      | Append  |
+------------------------------+---------+
| Update Frequency             | Daily   |
+------------------------------+---------+
| Credentials Required         | No      |
+------------------------------+---------+
| Uses Telescope Template      | Stream  |
+------------------------------+---------+
```

## Latest schema
``` eval_rst
.. csv-table::
   :file: ../schemas/crossref_events_latest.csv
   :width: 100%
   :header-rows: 1
```
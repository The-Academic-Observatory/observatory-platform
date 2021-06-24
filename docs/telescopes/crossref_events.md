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

See the crossref events [page](https://www.crossref.org/services/event-data/) for more information.

The corresponding table created in BigQuery are `crossref.crossref_events` and `crossref.crossref_events_partitions`.

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

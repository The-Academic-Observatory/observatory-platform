# Snapshot Telescope

A `SnapshotTelescope` is a specialisation of the `Telescope` class used to create tables for datasets that give full snapshots of data with each new release.  Each new release is saved in its own BigQuery table shard with timestamps to differentiate the releases.

Examples of snapshot telescopes found in the Academic Observatory include UCL Discovery, GRID, Google Analytics, Google Books, Crossref Metadata, OAPEN IRUS UK, ONIX, and Crossref FundRef.

## SnapshotRelease class

The `SnapshotRelease` class is a light wrapper for the `Release` class.  It constructs a `release_id` from the release date, and sets the `dag_id`,  `release_id`, download, extract, and transform file regex patterns. The constructor signature is:
```
def __init__(self, dag_id: str,
            release_date: pendulum.DateTime,
            download_files_regex: str = None,
            extract_files_regex: str = None,
            transform_files_regex: str = None)
```

## SnapshotTelescope class

The `SnapshotTelescope` class derives the `Telescope` class and implements 3 extra methods: `upload_transformed`, `bq_load`, `cleanup`.

### upload_transformed

This task uploads all the transformed files to the Google cloud storage bucket specified by the release object, for all releases processed.

### bq_load

This task loads each transformed release to a new BigQuery table for each release. The data used to load the table comes from the Google cloud storage bucket containing the transformed files.

It uses the transform path hierarchy specified in the transform_files in the release object to construct a table id, as well as the release date to find the correct table schema file to use.  The table names are suffixed by the release date.

Currently, it creates sharded BigQuery tables.

### cleanup

Deletes all the temporary files created by the Telescope locally during the ETL process.

See the API documentation for further details.


## Example

As an example use case, the Crossref Metadata telescope is a `SnapshotTelescope`, and adds tasks in the following order:
```
    self.add_setup_task(self.check_dependencies)    # User provided
    self.add_setup_task(self.check_release_exists)  # User provided
    self.add_task(self.download)                    # User provided
    self.add_task(self.upload_downloaded)           # User provided
    self.add_task(self.extract)                     # User provided
    self.add_task(self.transform)                   # User provided
    self.add_task(self.upload_transformed)          # From SnapshotTelescope
    self.add_task(self.bq_load)                     # From SnapshotTelescope
    self.add_task(self.cleanup)                     # From SnapshotTelescope
```
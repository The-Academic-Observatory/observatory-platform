# Stream Telescope

The `StreamTelescope` is a `Telescope` specialisation that deals with cases where there is an initial snapshot release of the entire dataset, followed by incremental updates.  The StreamTelescope outputs a main table that is a live snapshot of the current dataset, with all the updates reconciled.  Once the changes are merged, there is no way to rollback changes without recreating the table.

Examples of stream telescopes found in the Academic Observatory include OAPEN Metadata, DOAB, and Crossref Events.

## StreamRelease class

The `StreamRelease` class is a light wrapper for the `Release` class.  It constructs the `release_id` from the start date, and end date.  It sets the `dag_id`, `release_id`, `first_release`, `start_date`, `end_date`, download, extract, and transform regex patterns. The constructor signature is

```
def __init__(self, dag_id: str,
            start_date: pendulum.DateTime,
            end_date: pendulum.DateTime,
            first_release: bool,
            download_files_regex: str = None,
            extract_files_regex: str = None,
            transform_files_regex: str = None)
```

## StreamTelescope class

The `StreamTelescope` class derives the `Telescope` class and implements 6 additional methods.  These methods help setup a table with the full dataset which is continuously updated with the differential data by deleting old rows, and adding new rows to it.  These methods are: `get_release_info`, `upload_transformed`, `bq_load_partition`, `bq_delete_old`, `bq_append_new`, and `cleanup`.

### get_release_info

Creates a release instance and updates an XCOM with release date information.

### upload_transformed

This method uploads the transformed files to the Google cloud storage bucket.

### bq_load_partition

This method loads the transformed files from the Google cloud storage bucket for each release into a time partitioned BigQuery table.

### bq_delete_old

This method deletes rows from the table containing the full dataset marked for deletion by a differential update.

### bq_append_new

This method adds new rows to the table containing the full dataset from a differential update.

### cleanup

This method removes all temporary local files created during the ETL process.

See the API documentation for further details.

## Example

As an example use case, the Crossref Events is a `StreamTelescope`, and adds tasks in the following order:
```
    self.add_setup_task_chain([self.check_dependencies,     # User provided
                            self.get_release_info])         # From StreamTelescope
    self.add_task_chain([self.download,                     # User provided
                            self.upload_downloaded,         # User provided
                            self.transform,                 # User provided
                            self.upload_transformed,        # From StreamTelescope
                            self.bq_load_partition])        # From StreamTelescope
    self.add_task_chain([self.bq_delete_old,                # From StreamTelescope
                            self.bq_append_new,             # From StreamTelescope
                            self.cleanup], trigger_rule='none_failed')  # From StreamTelescope
```
import os
import re


class UnpaywallRelease:
    # example: https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/unpaywall_snapshot_2020-04-27T153236.jsonl.gz
    host = 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/'
    schema_gcs_object = 'unpaywall_schema.json'

    def __init__(self, url):
        self.url = url
        # Get basename of url without .gz extension
        friendly_name_ext = os.path.splitext(os.path.basename(self.url))[0]
        # Bucket file name can only contain alphanumeric characters and underscores
        self.friendly_name_ext = re.sub('[^0-9a-zA-Z]+', '_', os.path.splitext(friendly_name_ext)[0]) + \
                                 os.path.splitext(friendly_name_ext)[1]
        # Remove extension for table name
        self.friendly_name = os.path.splitext(self.friendly_name_ext)[0]


class CrossrefRelease:
    # example: https://api.crossref.org/snapshots/monthly/2019/12/all.json.tar.gz
    host = 'https://api.crossref.org/snapshots/monthly/'
    schema_gcs_object = 'crossref_schema.json'

    def __init__(self, url):
        self.url = url
        year = url.split('/')[-3]
        month = url.split('/')[-2]
        self.friendly_name_ext = f"crossref_{year}_{month}.json"
        # Remove extension for table name
        self.friendly_name = os.path.splitext(self.friendly_name_ext)[0]

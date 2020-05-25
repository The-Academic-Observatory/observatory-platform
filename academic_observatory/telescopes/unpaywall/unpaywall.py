import os
import re
import xml.etree.ElementTree as ET
from academic_observatory.utils.url_utils import retry_session
from academic_observatory.utils.data_utils import get_file
from academic_observatory.utils.ao_utils import ao_home, gcc_data


def list_unpaywall_releases():
    snapshot_list = []

    xml_string = retry_session().get(UnpaywallRelease.host).text
    if xml_string:
        # parse xml file and get list of snapshots
        root = ET.fromstring(xml_string)
        for unpaywall_release in root.findall('.//{http://s3.amazonaws.com/doc/2006-03-01/}Key'):
            snapshot_url = os.path.join(UnpaywallRelease.host, unpaywall_release.text)
            snapshot_list.append(snapshot_url)

    return snapshot_list


class UnpaywallRelease:
    # example: https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/unpaywall_snapshot_2020-04-27T153236.jsonl.gz
    host = 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/'
    #TODO get schema from github location instead
    schema_gcs_object = 'unpaywall_schema.json'
    debug_url = 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/unpaywall_snapshot_3000-01-27T153236.jsonl.gz'
    # Prepare paths
    download_path = ao_home('data-sources', 'unpaywall', 'downloaded')
    extracted_path = ao_home('data-sources', 'unpaywall', 'extracted')
    transformed_path = ao_home('data-sources', 'unpaywall', 'transformed')

    def __init__(self, url):
        self.url = url

        self.release_date = self.releasedate_from_url()
        self.compressed_file_name = self.compressed_filename_from_date()
        self.decompressed_file_name = self.decompressed_filename_from_date()
        self.table_name = self.tablename_from_date()

    def releasedate_from_url(self):
        date = re.search(r'\d{4}-\d{2}-\d{2}', self.url).group()

        return date

    def compressed_filename_from_date(self):
        compressed_file_name = f"unpaywall_{self.release_date}.jsonl.gz".replace('-', '_')

        return compressed_file_name

    def decompressed_filename_from_date(self):
        decompressed_file_name = f"unpaywall_{self.release_date}.jsonl".replace('-', '_')

        return decompressed_file_name

    def tablename_from_date(self):
        table_name = f"unpaywall_{self.release_date}".replace('-', '_')

        return table_name

    def download_unpaywall_release(self):
        get_file(fname=os.path.join(self.download_path, self.compressed_file_name), origin=self.url, cache_dir=self.download_path)

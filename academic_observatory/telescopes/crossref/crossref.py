import os
import re
import requests
from datetime import datetime
from academic_observatory.utils.url_utils import retry_session
from academic_observatory.utils.ao_utils import ao_home


def list_crossref_releases():
    snapshot_list = []

    # Loop through years and months
    for year in range(2018, int(datetime.today().strftime("%Y"))+1):
        for month in range(1, 12 + 1):
            snapshot_url = f"{os.path.join(CrossrefRelease.host,str(year),f'{month:02d}','all.json.tar.gz')}"
            response = retry_session().head(snapshot_url)
            if response:
                snapshot_list.append(snapshot_url)

    return snapshot_list


class CrossrefRelease:
    # example: https://api.crossref.org/snapshots/monthly/2019/12/all.json.tar.gz
    host = 'https://api.crossref.org/snapshots/monthly/'
    #TODO get schema from github location instead
    schema_gcs_object = 'crossref_schema.json'
    debug_url = 'https://api.crossref.org/snapshots/monthly/3000/01/all.json.tar.gz'
    # Prepare paths
    download_path = ao_home('data-sources', 'crossref', 'downloaded')
    extracted_path = ao_home('data-sources', 'crossref', 'extracted')
    transformed_path = ao_home('data-sources', 'crossref', 'transformed')

    def __init__(self, url):
        self.url = url

        self.release_date = self.releasedate_from_url()
        self.compressed_file_name = self.compressed_filename_from_date()
        self.decompressed_file_name = self.decompressed_filename_from_date()
        self.table_name = self.tablename_from_date()

    def releasedate_from_url(self):
        date = re.search(r'\d{4}/\d{2}', self.url).group()

        # create date string that can be parsed by pendulum
        date = date.replace('/', '-')

        return date

    def compressed_filename_from_date(self):
        compressed_file_name = f"crossref_{self.release_date}.json.tar.gz".replace('-', '_')

        return compressed_file_name

    def decompressed_filename_from_date(self):
        decompressed_file_name = f"crossref_{self.release_date}.jsonl".replace('-', '_')

        return decompressed_file_name

    def tablename_from_date(self):
        table_name = f"crossref_{self.release_date}".replace('-', '_')

        return table_name

    def download_crossref_release(self, header):
        response = requests.get(self.url, header=header)
        with open(os.path.join(self.download_path, self.compressed_file_name), 'wb') as out_file:
            out_file.write(response.content)

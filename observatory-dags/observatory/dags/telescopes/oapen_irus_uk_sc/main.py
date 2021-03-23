# Copyright 2020 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Aniek Roelofs

import calendar
import csv
import gzip
import io
import logging
import shutil
import subprocess
from datetime import datetime
from typing import List
from urllib.parse import ParseResult, quote, urlparse

import geoip2.database
import google.auth
import jsonlines
import requests
from geoip2.errors import AddressNotFoundError
from google.cloud import secretmanager, storage


def download(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    example = {
        "release_date": "2020-05",
        "publisher_info": {
            "name": "UCL Press",
            "project": "aniek-dev",
            "bucket": "test_cloud_function_123"
        },
        "blob_name": "oapen_access_stats.jsonl.gz"
    }
    request_json = request.get_json()
    release_date = request_json.get('release_date')  # 'YYYY-MM'
    publisher_info = request_json.get('publisher_info')  # e.g. 'UCL Press'
    blob_name = request_json.get('blob_name')

    # credentials and project
    credentials, project_id = google.auth.default()

    # secret manager client
    client = secretmanager.SecretManagerServiceClient()
    geoip_license_key = urlparse_secret(f'airflow-connections-geoip_license_key', project_id, client)
    oapen_irus_uk_api = urlparse_secret(f'airflow-connections-oapen_irus_uk_api', project_id, client)
    oapen_irus_uk_login = urlparse_secret(f'airflow-connections-oapen_irus_uk_login', project_id, client)

    # download geoip database
    geoip_client = download_geoip(geoip_license_key.password, '/tmp/geolite_city.tar.gz', '/tmp/geolite_city.mmdb')

    # download oapen access stats and replace ip addresses
    file_path = '/tmp/oapen_access_stats.jsonl.gz'
    publisher_name = quote(publisher_info["name"])
    logging.info(f'Downloading oapen access stats for month: {release_date}, publisher: {publisher_name}')
    if datetime.strptime(release_date, '%Y-%m') >= datetime(2020, 4, 1):
        download_access_stats_new(file_path, release_date, oapen_irus_uk_api, publisher_name, geoip_client)
    else:
        download_access_stats_old(file_path, release_date, oapen_irus_uk_login, publisher_name, geoip_client)

    # upload oapen access stats to bucket
    upload_to_bucket(file_path, blob_name, publisher_info)


def urlparse_secret(secret_name: str, project_id: str, client) -> ParseResult:
    request = {
        "name": f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    }
    response = client.access_secret_version(request)
    payload = response.payload.data.decode("UTF-8")
    uri_parts = urlparse(payload)
    return uri_parts


def download_geoip(geoip_license_key: str, download_path: str, extract_path: str):
    geolite_url = 'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=' \
                  f'{geoip_license_key}&suffix=tar.gz'
    # Download release in tar.gz format
    logging.info(f'Downloading geolite database file to: {download_path}')
    with requests.get(geolite_url, stream=True) as response:
        with open(download_path, 'wb') as file:
            shutil.copyfileobj(response.raw, file)

    # Tar file contains multiple files, use tar -ztf to get path of 'GeoLite2-City.mmdb'
    # Use this path to extract only GeoLite2-City.mmdb to a new file.
    logging.info(f'Extracting geolite database file to: {extract_path}')
    cmd = f"registry_path=$(tar -ztf {download_path} | grep -m1 '/GeoLite2-City.mmdb'); " \
          f"tar -xOzf {download_path} $registry_path > {extract_path}"
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
    output, error = proc.communicate()

    geoip_client = geoip2.database.Reader(extract_path)
    return geoip_client


def download_access_stats_old(file_path: str, release_date: str, oapen_irus_uk_login: ParseResult, publisher_name: str,
                              geoip_client: geoip2.database.Reader):
    start_date = release_date + "-01"
    year, month = release_date.split('-')
    end_date = release_date + "-" + calendar.monthrange(int(year), int(month))
    url = f'https://irus.jisc.ac.uk/IRUSConsult/irus-oapen/v2/br1b/?frmRepository=1%7COAPEN+Library&frmPublisher' \
          f'={publisher_name}&frmFrom={start_date}&frmTo={end_date}&frmFormat=TSV&Go=Generate+Report'

    # start a requests session
    s = requests.Session()

    # login
    login_response = s.post('https://irus.jisc.ac.uk/IRUSConsult/irus-oapen/v2/?action=login', data={
        'email': oapen_irus_uk_login.username,
        'password': oapen_irus_uk_login.password,
        'action': 'login'
    })
    if 'After you have finished your session please remember to' in login_response.text:
        logging.info('Successfully logged in at https://irus.jisc.ac.uk/IRUSConsult/irus-oapen/v2/?action=login')
    else:
        raise RuntimeError(f'Login at https://irus.jisc.ac.uk/IRUSConsult/irus-oapen/v2/?action=login unsuccessful')

    # get tsv file
    response = s.get(url)
    if response.status_code == 200 and response.text.startswith('"Book Report 1b (BR1b)"'):
        logging.info('Successfully downloaded tsv file with item requests data')
    else:
        raise RuntimeError(f'')
    content = response.content.decode('utf-8').splitlines()

    # get publisher and begin & end date
    publisher = content[1].strip('"')
    begin_date, end_date = content[3].strip('"').split(' to ')

    # store results in csv dictreader
    csv_reader = csv.DictReader(content[6:7] + content[8:], delimiter='\t')
    csv_entries = [{k: v for k, v in row.items()} for row in csv_reader]

    all_results = []
    for entry in csv_entries:
        proprietary_id = entry['Proprietary Identifier']
        book_title = entry['Title']
        grant = entry['Grant']
        grant_number = entry['Grant Number']
        doi = entry['DOI']
        isbn = entry['ISBN'].strip('ISBN ')
        title_requests = entry['Reporting Period Total']

        client_ip = entry['IP Address']
        client_lat, client_lon, client_city, client_country, client_country_code = replace_ip_address(client_ip,
                                                                                                      geoip_client)

        result = {
            'proprietary_id': proprietary_id,
            'book_title': book_title,
            'grant': grant,
            'grant_number': grant_number,
            'doi': doi,
            'isbn': isbn,
            'client_lat': client_lat,
            'client_lon': client_lon,
            'client_city': client_city,
            'client_country': client_country,
            'client_country_code': client_country_code,
            'publisher': publisher,
            'begin_date': begin_date,
            'end_date': end_date,
            'title_requests': title_requests,
        }
        result = {k: None if not v else v for k, v in result.items()}
        all_results.append(result)
    logging.info(f'Found {len(all_results)} access stats entries')
    list_to_jsonl_gz(file_path, all_results)


def download_access_stats_new(file_path: str, release_date: str, oapen_irus_uk_api: ParseResult, publisher_name: str,
                              geoip_client: geoip2.database.Reader):
    # create url
    requestor_id = oapen_irus_uk_api.username
    api_key = oapen_irus_uk_api.password
    url = f'https://irus.jisc.ac.uk/sushiservice/oapen/reports/oapen_ir/?requestor_id={requestor_id}' \
          f'&platform=215&begin_date={release_date}&end_date={release_date}&formatted&api_key={api_key}' \
          f'&attributes_to_show=Client_IP%7CCountry&publisher={publisher_name}'

    response = requests.get(url)
    if response.status_code != 200:
        raise RuntimeError(f'Request unsuccessful, status code: {response.status_code}, response: {response.text}, '
                           f'reason: {response.reason}')
    logging.info('Download successful, replacing IP addresses with info from geolite database.')
    response_json = response.json()

    all_results = []
    for item in response_json['Report_Items']:
        item_id = item['Item_ID']['URI']
        if item_id.startswith('http://library.oapen.org/handle/'):
            item_id = item_id[len('http://library.oapen.org/handle/'):]
        book_title = item['Item']
        publisher = item['Publisher']
        for client in item['Performance']:
            begin_date = client['Period']['Begin_Date']
            end_date = client['Period']['End_Date']
            total_item_investigations = client['Instance']['Total_Item_Investigations']
            total_item_requests = client['Instance']['Total_Item_Requests']
            unique_item_investigations = client['Instance']['Unique_Item_Investigations']
            unique_item_requests = client['Instance']['Unique_Item_Requests']
            country = client['Instance'].get('Country', {
                'Country_Name': ''
            })['Country_Name']
            country_code = client['Instance'].get('Country', {
                'Country_Code': ''
            })['Country_Code']

            client_ip = client['Instance']['Client_IP']
            client_lat, client_lon, client_city, client_country, client_country_code = replace_ip_address(client_ip,
                                                                                                          geoip_client)
            result = {
                'item_id': item_id,
                'book_title': book_title,
                'country': country,
                'country_code': country_code,
                'client_lat': client_lat,
                'client_lon': client_lon,
                'client_city': client_city,
                'client_country': client_country,
                'client_country_code': client_country_code,
                'publisher': publisher,
                'begin_date': begin_date,
                'end_date': end_date,
                'total_item_investigations': total_item_investigations,
                'total_item_requests': total_item_requests,
                'unique_item_investigations': unique_item_investigations,
                'unique_item_requests': unique_item_requests,
            }
            all_results.append(result)
    logging.info(f'Found {len(all_results)} access stats entries, for {len(response_json["Report_Items"])} books')
    list_to_jsonl_gz(file_path, all_results)


def replace_ip_address(client_ip: str, geoip_client: geoip2.database.Reader):
    """

    :param client_ip:
    :param geoip_client:
    :return:
    """
    try:
        geoip_response = geoip_client.city(client_ip)
    except AddressNotFoundError:
        return '', '', '', '', ''

    client_lat = geoip_response.location.latitude
    client_lon = geoip_response.location.longitude
    client_city = geoip_response.city.name
    client_country = geoip_response.country.name
    client_country_code = geoip_response.country.iso_code

    return client_lat, client_lon, client_city, client_country, client_country_code


def list_to_jsonl_gz(file_path: str, list_of_dicts: List[dict]):
    """ Takes a list of dictionaries and writes this to a gzipped jsonl file.
    :param file_path: Path to the .jsonl.gz file
    :param list_of_dicts: A list containing dictionaries that can be written out with jsonlines
    :return: None.
    """
    logging.info(f'Writing results to file: {file_path}')
    with io.BytesIO() as bytes_io:
        with gzip.GzipFile(fileobj=bytes_io, mode='w') as gzip_file:
            with jsonlines.Writer(gzip_file) as writer:
                writer.write_all(list_of_dicts)

        with open(file_path, 'wb') as jsonl_gzip_file:
            jsonl_gzip_file.write(bytes_io.getvalue())


def upload_to_bucket(file_path: str, blob_name: str, publisher_info: dict):
    storage_client = storage.Client(publisher_info['project'])
    bucket = storage_client.bucket(publisher_info['bucket'])
    blob = bucket.blob(blob_name)

    logging.info(f'Uploading file "{file_path}". Blob: {blob_name}, bucket: {publisher_info["bucket"]}, project:'
                 f' {publisher_info["project"]}')
    blob.upload_from_filename(file_path)

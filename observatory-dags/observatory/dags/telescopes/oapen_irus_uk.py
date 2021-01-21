import json
import os
import pickle
import shutil
import subprocess
from datetime import datetime

import geoip2.database
import requests
from geoip2.errors import AddressNotFoundError


class OapenIrusUKRelease:
    def __init__(self, start_date: datetime, end_date: datetime):
        self.start_date = start_date.strftime("%Y-%m")
        self.end_date = end_date.strftime("%Y-%m")
        self.url = OapenIrusUk.url.format(requestor_id=OapenIrusUk.requestor_id, start=self.start_date,
                                          end=self.end_date, api_key=OapenIrusUk.api_key)


class OapenIrusUk:
    requestor_id = ''
    api_key = ''
    geoip_license_key = ''
    geoip_userid = 0
    url = 'https://irus.jisc.ac.uk/sushiservice/oapen/reports/oapen_ir/?requestor_id={requestor_id}' \
          '&platform=215&begin_date={start}&end_date={end}&formatted&api_key={' \
          'api_key}&attributes_to_show=Client_IP%7CCountry'
    # 'item_id=hdl:20.500.12657/25757'
    geolite_download_path = os.path.join('geolite_city.tar.gz')
    geolite_database_path = os.path.join('geolite_city.mmdb')


release = OapenIrusUKRelease(datetime(2020, 4, 1), datetime(2020, 4, 1))


def download_geoip():
    geolite_url = 'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=' \
                  f'{OapenIrusUk.geoip_license_key}&suffix=tar.gz'
    # Download release in tar.gz format
    with requests.get(geolite_url, stream=True) as response:
        with open(OapenIrusUk.geolite_download_path, 'wb') as file:
            shutil.copyfileobj(response.raw, file)

    # Tar file contains multiple files, use tar -ztf to get path of 'GeoLite2-City.mmdb'
    # Use this path to extract only GeoLite2-City.mmdb to a new file.
    cmd = f"registry_path=$(tar -ztf {OapenIrusUk.geolite_download_path} | grep -m1 '/GeoLite2-City.mmdb'); " \
          f"tar -xOzf {OapenIrusUk.geolite_download_path} $registry_path > {OapenIrusUk.geolite_database_path}"
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
    output, error = proc.communicate()
    output = output.decode('utf-8')
    error = error.decode('utf-8')


def download():
    test = True
    if test:
        with open('/Users/284060a/test_files/oapen_response.pickle', 'rb') as handle:
            response_json = pickle.load(handle)
    else:
        response = requests.get(release.url)
        response_json = response.json()
    geoip_client = geoip2.database.Reader(OapenIrusUk.geolite_database_path)

    country_name_found = 0
    country_code_found = 0
    country_name_not_found = 0
    country_code_not_found = 0
    address_found = 0
    address_not_found = 0
    all_results = []
    for item in response_json['Report_Items']:
        item_id = item['Item_ID']['URI']
        if item_id.startswith('http://library.oapen.org/handle/'):
            item_id = item_id[len('http://library.oapen.org/handle/'):]
        book_title = item['Item']
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
            # try:
            #     country = client['Instance']['Country']['Country_Name']
            #     country_name_found += 1
            # except KeyError:
            #     country_name_not_found += 1
            #     country = ''
            # try:
            #     country_code = client['Instance']['Country']['Country_Code']
            #     country_code_found += 1
            # except KeyError:
            #     country_code = ''
            #     country_code_not_found +=1

            client_ip = client['Instance']['Client_IP']
            try:
                geoip_response = geoip_client.city(client_ip)
                client_lat = geoip_response.location.latitude
                client_lon = geoip_response.location.longitude
                client_city = geoip_response.city.name
                client_country = geoip_response.country.name
                client_country_code = geoip_response.country.iso_code
                address_found += 1
            except AddressNotFoundError:
                address_not_found += 1
                client_lat = ''
                client_lon = ''
                client_city = ''
                client_country = ''
                client_country_code = ''

            result = {
                'item_id': item_id,
                'book_title': book_title,
                'begin_date': begin_date,
                'end_date': end_date,
                'total_item_investigations': total_item_investigations,
                'total_item_requests': total_item_requests,
                'unique_item_investigations': unique_item_investigations,
                'unique_item_requests': unique_item_requests,
                'country': country,
                'country_code': country_code,
                'client_lat': client_lat,
                'client_lon': client_lon,
                'client_city': client_city,
                'client_country': client_country,
                'client_country_code': client_country_code
            }
            all_results.append(result)
    print(address_not_found, address_found, address_not_found + address_found)
    print(country_name_not_found, country_name_found, country_name_not_found + country_name_found)
    print(country_code_not_found, country_code_found, country_code_not_found + country_code_found)
    print(len(all_results))
    with open('/Users/284060a/test_files/oapen_result.json', 'w') as fp:
        json.dump(all_results, fp)


def upload():
    pass


if __name__ == '__main__':
    # download_geoip()
    download()
    upload()

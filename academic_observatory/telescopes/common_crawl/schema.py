# Copyright 2019 Curtin University
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

# Author: James Diprose

import datetime
from typing import List


class WarcIndexInfo:

    def __init__(self, fetch_month: datetime.date, url_surtkey: str, url_host_name: str, url_host_tld: str,
                 url_host_2nd_last_part: str, url_host_3rd_last_part: str, url_host_4th_last_part: str,
                 url_host_5th_last_part: str, url_host_registry_suffix: str, url_host_registered_domain: str,
                 url_host_private_suffix: str, url_host_private_domain: str, url_protocol: str, url_port: int,
                 url_path: str, url_query: str, fetch_time: datetime.datetime, fetch_status: int, content_digest: str,
                 content_mime_type: str, content_mime_detected: str, content_charset: str, content_languages: str):
        self.fetch_month = fetch_month
        self.url_surtkey = url_surtkey
        self.url_host_name = url_host_name
        self.url_host_tld = url_host_tld
        self.url_host_2nd_last_part = url_host_2nd_last_part
        self.url_host_3rd_last_part = url_host_3rd_last_part
        self.url_host_4th_last_part = url_host_4th_last_part
        self.url_host_5th_last_part = url_host_5th_last_part
        self.url_host_registry_suffix = url_host_registry_suffix
        self.url_host_registered_domain = url_host_registered_domain
        self.url_host_private_suffix = url_host_private_suffix
        self.url_host_private_domain = url_host_private_domain
        self.url_protocol = url_protocol
        self.url_port = url_port
        self.url_path = url_path
        self.url_query = url_query
        self.fetch_time = fetch_time
        self.fetch_status = fetch_status
        self.content_digest = content_digest
        self.content_mime_type = content_mime_type
        self.content_mime_detected = content_mime_detected
        self.content_charset = content_charset
        self.content_languages = content_languages

    @staticmethod
    def from_dict(dict_: dict):
        return WarcIndexInfo(dict_["fetch_month"], dict_["url_surtkey"], dict_["url_host_name"],
                             dict_["url_host_tld"], dict_["url_host_2nd_last_part"], dict_["url_host_3rd_last_part"],
                             dict_["url_host_4th_last_part"], dict_["url_host_5th_last_part"],
                             dict_["url_host_registry_suffix"], dict_["url_host_registered_domain"],
                             dict_["url_host_private_suffix"], dict_["url_host_private_domain"], dict_["url_protocol"],
                             dict_["url_port"], dict_["url_path"], dict_["url_query"], dict_["fetch_time"],
                             dict_["fetch_status"], dict_["content_digest"], dict_["content_mime_type"],
                             dict_["content_mime_detected"], dict_["content_charset"], dict_["content_languages"])


class WarcIndex:

    def __init__(self, url: str, warc_filename: str, warc_record_offset: int, warc_record_length: int):
        self.url = url
        self.warc_filename = warc_filename
        self.warc_record_offset = warc_record_offset
        self.warc_record_length = warc_record_length

    @staticmethod
    def from_dict(dict_: dict):
        return WarcIndex(dict_["url"], dict_["warc_filename"], dict_["warc_record_offset"], dict_["warc_record_length"])


class InstitutionIndex:

    def __init__(self, institution_id: str, institution_name: str, institution_type: str, institution_url: str,
                 institution_country_code: str):
        self.institution_id = institution_id
        self.institution_name = institution_name
        self.institution_type = institution_type
        self.institution_url = institution_url
        self.institution_country_code = institution_country_code

    @staticmethod
    def from_dict(dict_: dict):
        return InstitutionIndex(dict_["grid_id"], dict_["institute_name"], dict_["institute_type"],
                                dict_["institute_url"], dict_["institute_country_code"])


class Link:

    def __init__(self, url: str, text: str, institution_id: str, type: str):
        self.url = url
        self.text = text
        self.institution_id = institution_id
        self.type = type

    def to_dict(self):
        return {
            "url": self.url,
            "text": self.text,
            "institution_id": self.institution_id,
            "type": self.type
        }


class PageInfo:

    def __init__(self, institution_index: InstitutionIndex, warc_index: WarcIndex, warc_index_info: WarcIndexInfo,
                 title: str = None, links: List[Link] = [], text_content: str = None, raw_content: str = None):
        self.institution_index = institution_index
        self.warc_index = warc_index
        self.warc_index_info = warc_index_info
        self.title = title
        self.links = links
        self.text_content = text_content
        self.raw_content = raw_content

    def to_dict(self):
        links_json = [link.to_dict() for link in self.links]

        return {
            "institution_id": self.institution_index.institution_id,
            "institution_name": self.institution_index.institution_name,
            "institution_type": self.institution_index.institution_type,
            "institution_url": self.institution_index.institution_url,
            "institution_country_code": self.institution_index.institution_country_code,
            "fetch_month": self.warc_index_info.fetch_month,
            "url_surtkey": self.warc_index_info.url_surtkey,
            "url": self.warc_index.url,
            "url_host_name": self.warc_index_info.url_host_name,
            "url_host_tld": self.warc_index_info.url_host_tld,
            "url_host_2nd_last_part": self.warc_index_info.url_host_2nd_last_part,
            "url_host_3rd_last_part": self.warc_index_info.url_host_3rd_last_part,
            "url_host_4th_last_part": self.warc_index_info.url_host_4th_last_part,
            "url_host_5th_last_part": self.warc_index_info.url_host_5th_last_part,
            "url_host_registry_suffix": self.warc_index_info.url_host_registry_suffix,
            "url_host_registered_domain": self.warc_index_info.url_host_registered_domain,
            "url_host_private_suffix": self.warc_index_info.url_host_private_suffix,
            "url_host_private_domain": self.warc_index_info.url_host_private_domain,
            "url_protocol": self.warc_index_info.url_protocol,
            "url_port": self.warc_index_info.url_port,
            "url_path": self.warc_index_info.url_path,
            "url_query": self.warc_index_info.url_query,
            "fetch_time": self.warc_index_info.fetch_time,
            "fetch_status": self.warc_index_info.fetch_status,
            "content_digest": self.warc_index_info.content_digest,
            "content_mime_type": self.warc_index_info.content_mime_type,
            "content_mime_detected": self.warc_index_info.content_mime_detected,
            "content_charset": self.warc_index_info.content_charset,
            "content_languages": self.warc_index_info.content_languages,
            "warc_filename": self.warc_index.warc_filename,
            "warc_record_offset": self.warc_index.warc_record_offset,
            "warc_record_length": self.warc_index.warc_record_length,
            "title": self.title,
            "links": links_json,
            "text_content": self.text_content,
            "raw_content": self.raw_content
        }

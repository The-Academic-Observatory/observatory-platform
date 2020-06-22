# The MIT License (MIT)
#
# Copyright (c) 2017 Common Crawl
# Copyright (c) 2019 Curtin University
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# Much of the contents of this file originate from: https://github.com/commoncrawl/cc-pyspark/blob/master/sparkcc.py

import logging
from io import BytesIO
from typing import List, Union, Tuple

import boto3
import botocore
import pandas as pd
import ray
from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

from academic_observatory.telescopes.common_crawl.schema import WarcIndex, Link
from academic_observatory.telescopes.grid_old import load_grid_index
from academic_observatory.utils import HtmlParser, get_url_domain_suffix


@ray.remote
class CCFullTextFetcher:

    def __init__(self, grid_index_path=None, url_index_path=None, warc_parse_http_header=True):
        no_sign_request = botocore.client.Config(signature_version=botocore.UNSIGNED)
        self.s3_client = boto3.client('s3', config=no_sign_request)
        self.bucketname = "commoncrawl"
        self.warc_parse_http_header = warc_parse_http_header

        self.grid_index = load_grid_index(grid_index_path)
        self.url_index = self._get_url_index(url_index_path)

    def _get_url_index(self, url_index_path):
        url_index = dict()

        if url_index_path is not None:
            df = pd.read_csv(url_index_path, names=['url', 'name'])
            for i, row in df.iterrows():
                url = row['url']
                name = row['name']
                url_index[url] = name
        else:
            logging.warning("No url_index_path specified so url_index not loaded.")

        return url_index

    def _process_warc_record(self, record, warc_index):
        if record.rec_type != 'response':
            return None
        content_type = record.http_headers.get_header('content-type', None)
        if content_type is None or 'html' not in content_type:
            return None

        page = record.content_stream().read()
        parser = HtmlParser(page)
        title = parser.get_title()
        links_ = parser.get_links()
        text = parser.get_full_text()
        raw_content = parser.get_raw_content()

        self_domain_suffix = get_url_domain_suffix(warc_index.url)

        # Create Link objects
        links = []
        for url, text in links_:
            domain_suffix = get_url_domain_suffix(url)
            link_institution_id = None

            # Get institution_id
            if domain_suffix in self.grid_index:
                link_institution_id = self.grid_index[domain_suffix]

            # If link url matches the page's url then set link type to self
            if self_domain_suffix == domain_suffix:
                link_type = 'self'
            elif domain_suffix in self.url_index:
                link_type = self.url_index[domain_suffix]
            elif link_institution_id is not None:
                link_type = 'institution'
            else:
                link_type = 'web'

            link = Link(url, text, link_institution_id, link_type)
            links.append(link)

        return title, links, text, raw_content

    @ray.method(num_return_vals=1)
    def fetch_page(self, warc_index: WarcIndex) -> Union[None, Tuple]:
        """ For a particular WarcIndex, fetch the title, links, text and raw_content.

        :param warc_index: the WarcIndex to fetch data about.
        :return: the title, links, text and raw_content of a WarcIndex.
        """

        warc_path = warc_index.warc_filename
        offset = warc_index.warc_record_offset
        length = warc_index.warc_record_length
        no_parse = (not self.warc_parse_http_header)

        logging.debug(f"Fetching WARC record for ({warc_path}, offset: {offset}, length: {length})")
        rangereq = f'bytes={offset}-{offset + length - 1}'
        try:
            response = self.s3_client.get_object(Bucket=self.bucketname, Key=warc_path, Range=rangereq)
        except botocore.client.ClientError as exception:
            logging.error(f'Failed to download: ({warc_path}, offset: {offset}, length: {length}) - {exception}')
            return None

        record_stream = BytesIO(response["Body"].read())
        page_infos: List[Union[Tuple, None]] = []
        try:
            for record in ArchiveIterator(record_stream, no_record_parse=no_parse):
                page_info = self._process_warc_record(record, warc_index)
                page_infos.append(page_info)
        except ArchiveLoadFailed as exception:
            logging.error(f'Invalid WARC record: ({warc_path}, offset: {offset}, length: {length}) - {exception}')
            return None

        if len(page_infos) > 1:
            logging.warning(
                f'Multiple WARC records returned for: ({warc_path}, offset: {offset}, length: {length})')

        return page_infos[0]

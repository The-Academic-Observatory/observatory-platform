#
# Copyright 2019 Curtin University. All rights reserved.
#
# Author: James Diprose
#

import argparse
import logging
from multiprocessing import cpu_count

from academic_observatory.telescopes.common_crawl import cc_fetch_full_text
from academic_observatory.telescopes.grid import download_grid_dataset, index_grid_dataset
from academic_observatory.telescopes.oai_pmh import fetch_endpoints, FETCH_ENDPOINTS_PROCESS_MULTIPLIER, fetch_records
from academic_observatory.utils import validate_path, validate_datetime


def main():
    """ Contains the definition of the argparse command line interface for ao_util, the Academic Observatory command
    line tool.
    :return: None
    """

    logging.basicConfig(level=logging.INFO)

    # Parse arguments
    root_parser = argparse.ArgumentParser(description='The Academic Observatory command line tool')

    # Sub parsers
    root_parsers = root_parser.add_subparsers(title='Datasets',
                                              description='grid: GRID dataset, '
                                                          'oai_pmh: OAI-PMH dataset, '
                                                          'common_crawl: Common Crawl dataset',
                                              dest='dataset',
                                              required=True)

    ############################
    # GRID command
    ############################

    parser_grid = root_parsers.add_parser('grid')
    grid_parsers = parser_grid.add_subparsers(title='GRID dataset commands',
                                              description='download: download the GRID dataset, '
                                                          'index: build an index of the GRID dataset',
                                              dest='cmd',
                                              required=True)
    parser_grid_download = grid_parsers.add_parser('download')
    parser_grid_index = grid_parsers.add_parser('index')

    # GRID download sub commands
    parser_grid_download.add_argument('-o',
                                      '--output',
                                      type=validate_path,
                                      required=False,
                                      help='The output directory where to save the dataset, by default the dataset '
                                           'is saved in the Academic Observatory directory ~/.academic-observatory.')
    parser_grid_download.add_argument('-np',
                                      '--num_processes',
                                      type=int,
                                      default=cpu_count(),
                                      help='The number of processes to use when processing jobs. By default it is the'
                                           'number of CPU cores.')
    parser_grid_download.add_argument('-l',
                                      '--local_mode',
                                      action='store_true',
                                      help='Whether to run the program serially.')
    parser_grid_download.add_argument('-t',
                                      '--timeout',
                                      type=float,
                                      default=10.,
                                      help='The timeout to use when fetching resources over the network.')

    parser_grid_download.set_defaults(func=lambda args_: download_grid_dataset(args_.output, args_.num_processes,
                                                                               args_.local_mode, args_.timeout))

    # GRID index sub commands
    parser_grid_index.add_argument('-i',
                                   '--input',
                                   type=validate_path,
                                   required=False,
                                   help='The directory of the GRID dataset. By default it will look for the GRID '
                                        'dataset in the Academic Observatory directory ~/.academic-observatory and '
                                        'if the GRID dataset is not found it will downloaded.')
    parser_grid_index.add_argument('-o',
                                   '--output',
                                   type=argparse.FileType('w'),
                                   required=False,
                                   help='The path and filename of the CSV file to save the resulting GRID index. '
                                        'By default it will be saved in the Academic Observatory directory '
                                        '~/.academic-observatory')
    parser_grid_index.set_defaults(func=lambda args_: index_grid_dataset(args_.input, args_.output))

    ############################
    # OAI-PMH command
    ############################

    parser_oai = root_parsers.add_parser('oai_pmh')
    oai_parsers = parser_oai.add_subparsers(title='OAI-PMH dataset commands',
                                            description='fetch_endpoints: verify that a list of OAI-PMH endpoint '
                                                        'URLs are valid endpoints and fetch their meta-data, '
                                                        'fetch_records: fetch the records from a list of '
                                                        'OAI-PMH endpoints',
                                            dest='cmd',
                                            required=True)
    parser_oai_endpoints = oai_parsers.add_parser('fetch_endpoints')
    parser_oai_records = oai_parsers.add_parser('fetch_records')

    # Fetch endpoints sub command
    parser_oai_endpoints.add_argument('-i',
                                      '--input',
                                      type=argparse.FileType('r'),
                                      required=True,
                                      help='The path to the CSV file that contains the list of potential OAI-PMH '
                                           'endpoint URLs.')
    parser_oai_endpoints.add_argument('-k',
                                      '--key',
                                      type=str,
                                      required=True,
                                      help='The name of the column that contains the OAI-PMH endpoint URLs in the '
                                           'input CSV.')
    parser_oai_endpoints.add_argument('-o',
                                      '--output',
                                      type=validate_path,
                                      required=False,
                                      help='The path to the directory where the results will be saved.')
    parser_oai_endpoints.add_argument('-np',
                                      '--num_processes',
                                      type=int,
                                      default=cpu_count() * FETCH_ENDPOINTS_PROCESS_MULTIPLIER,
                                      help='The number of processes to use when processing jobs. By default it is the'
                                           'number of CPU cores multiplied by 16.')
    parser_oai_endpoints.add_argument('-l',
                                      '--local_mode',
                                      action='store_true',
                                      help='Whether to run the program serially.')
    parser_oai_endpoints.add_argument('-t',
                                      '--timeout',
                                      type=float,
                                      default=10.,
                                      help='The timeout to use when fetching resources over the network.')
    parser_oai_endpoints.set_defaults(func=lambda args_: fetch_endpoints(args_.input, args_.key, args_.output,
                                                                         args_.num_processes, args_.local_mode,
                                                                         args_.timeout))

    # Fetch records sub command
    parser_oai_records.add_argument('-s',
                                    '--start_date',
                                    type=lambda date_str: validate_datetime(date_str, "%Y-%m-%d"),
                                    required=True,
                                    help='The date to fetching records from, in the format YYYY-MM-DD.')
    parser_oai_records.add_argument('-e',
                                    '--end_date',
                                    type=lambda date_str: validate_datetime(date_str, "%Y-%m-%d"),
                                    required=True,
                                    help='The date to fetching records from, in the format YYYY-MM-DD.')
    parser_oai_records.add_argument('-i',
                                    '--input',
                                    type=argparse.FileType('r'),
                                    required=False,
                                    help='The path to the CSV file that contains the list of OAI-PMH endpoints.')
    parser_oai_records.add_argument('-o',
                                    '--output',
                                    type=validate_path,
                                    required=False,
                                    help='The path to the directory where the results should be saved.')
    parser_oai_records.add_argument('-np',
                                    '--num_processes',
                                    type=int,
                                    default=cpu_count(),
                                    help='The number of processes to use when processing jobs.')
    parser_oai_records.add_argument('-l',
                                    '--local_mode',
                                    action='store_true',
                                    help='Whether to run the program serially.')
    parser_oai_records.add_argument('-t',
                                    '--timeout',
                                    type=float,
                                    default=10.,
                                    help='The timeout to use when fetching resources over the network.')
    parser_oai_records.set_defaults(func=lambda args_: fetch_records(args_.start_date, args_.end_date,
                                                                     args_.input, args_.output, args_.num_processes,
                                                                     args_.local_mode, args_.timeout))

    ############################
    # OAI-PMH command
    ############################

    parser_cc = root_parsers.add_parser('common_crawl')
    cc_parsers = parser_cc.add_subparsers(title='Common Crawl dataset commands',
                                          description='fetch_full_text: Fetch full text from Common Crawl, '
                                                      'pre-process and save as gzipped newline delimited JSON files',
                                          dest='cmd',
                                          required=True)
    parser_cc_full_text = cc_parsers.add_parser('fetch_full_text')

    parser_cc_full_text.add_argument('-tn',
                                     '--table_name',
                                     type=str,
                                     required=True,
                                     help='The BigQuery table to fetch the records from, in the format '
                                          'project_name.dataset_name.table_name')
    parser_cc_full_text.add_argument('-s',
                                     '--start_date',
                                     type=lambda date_str: validate_datetime(date_str, "%Y-%m"),
                                     required=True,
                                     help='The month to begin fetching records from, in the format YYYY-MM. Only the '
                                          'year and month are required because the crawls are partitioned by month in '
                                          'BigQuery. For example, if the crawl you are interested in occurred on '
                                          '2019-09-14, then specify 2019-09 as the start date.')
    parser_cc_full_text.add_argument('-e',
                                     '--end_date',
                                     type=lambda date_str: validate_datetime(date_str, "%Y-%m"),
                                     required=True,
                                     help='The month to finish fetching records from, in the format YYYY-MM. Only the '
                                          'year and month are required because the crawls are partitioned by month in '
                                          'BigQuery. For example, if the crawl you are interested in occurred on '
                                          '2019-09-14, then specify 2019-09 as the end date.')
    parser_cc_full_text.add_argument('-o',
                                     '--output',
                                     type=validate_path,
                                     required=False,
                                     help='The path to the directory where the results will be saved.')
    parser_cc_full_text.add_argument('-g',
                                     '--grid_index',
                                     type=argparse.FileType('r'),
                                     required=False,
                                     help='The path to the GRID index csv file, if not specified the default will be '
                                          'used and downloaded automatically if it doesn\'t exist.')
    parser_cc_full_text.add_argument('-u',
                                     '--url_type_index',
                                     type=argparse.FileType('r'),
                                     required=False,
                                     help='The path to the url type index csv file.')
    parser_cc_full_text.add_argument('-np',
                                     '--num_processes',
                                     type=int,
                                     default=cpu_count(),
                                     help='The number of processes to use when processing jobs.')
    parser_cc_full_text.add_argument('-l',
                                     '--local_mode',
                                     action='store_true',
                                     help='Whether to run the program serially.')
    parser_cc_full_text.add_argument('-t',
                                     '--timeout',
                                     type=float,
                                     default=10.,
                                     help='The timeout to use when fetching resources over the network.')

    parser_cc_full_text.set_defaults(func=lambda args_: cc_fetch_full_text(args_.table_name, args_.start_date,
                                                                           args_.end_date, args_.output,
                                                                           args_.grid_index, args_.url_type_index,
                                                                           args_.num_processes,
                                                                           args_.local_mode, args_.timeout))

    # Parse arguments and call function that was set with set_defaults
    args = root_parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

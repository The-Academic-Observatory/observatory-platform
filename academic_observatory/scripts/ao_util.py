import argparse
import logging
from multiprocessing import cpu_count

from academic_observatory.grid import download_grid_dataset, index_grid_dataset
from academic_observatory.utils.argparse_utils import validate_path


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
    parser_grid = root_parsers.add_parser('grid')
    # parser_oai = root_parsers.add_parser('oai_pmh')
    # parser_cc = root_parsers.add_parser('common_crawl')

    # GRID command
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

    # Global arguments
    for parser in [parser_grid_download, parser_grid_index]:
        parser.add_argument('-l',
                            '--local_mode',
                            action='store_true',
                            help='Whether to run the program serially.')
        parser.add_argument('-np',
                            '--num_processes',
                            type=int,
                            default=cpu_count(),
                            help='The number of processes to use when processing jobs.')
        parser.add_argument('-t',
                            '--timeout',
                            type=float,
                            default=10.,
                            help='The timeout to use when fetching resources over the network.')

    # Parse arguments and call function that was set with set_defaults
    args = root_parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

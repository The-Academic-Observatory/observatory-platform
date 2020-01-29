import argparse
import logging
import os


def validate_path(string: str):
    """ Validate whether the given string is a valid path.
    :param string: a string that might be a file path.
    :return: the string if it validated otherwise raise an argparse.ArgumentTypeError.
    """

    if not os.path.exists(string):
        msg = f"Path does not exist: {string}"
        logging.error(msg)
        raise argparse.ArgumentTypeError(msg)
    else:
        return string

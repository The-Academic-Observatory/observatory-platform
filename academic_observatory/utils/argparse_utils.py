import argparse
import logging
import os


def validate_path(string: str):
    if not os.path.exists(string):
        msg = f"File does not exist: {string}"
        logging.error(msg)
        raise argparse.ArgumentTypeError(msg)
    else:
        return string

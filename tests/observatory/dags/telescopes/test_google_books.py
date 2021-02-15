import json
import logging
import os
import pathlib
import shutil
import unittest
from datetime import datetime
from unittest.mock import patch

import pendulum
import vcr
from airflow.exceptions import AirflowException
from click.testing import CliRunner

from observatory.dags.telescopes.google_books import download_reports, GoogleBooksRelease, GoogleBooksTelescope
from observatory.platform.utils.config_utils import SubFolder, telescope_path
from observatory.platform.utils.data_utils import _hash_file


class TestGoogleBooks(unittest.TestCase):
    """ Tests for the functions used by the google books telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestGoogleBooks, self).__init__(*args, **kwargs)

        # self.start_date = pendulum.parse('2020-01-02 11:48:23.795099+00:00')
        self.start_date = pendulum.parse('2020-01-02')
        self.end_date = pendulum.parse('2020-02-02')
        self.account_number = '123456789'
        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()

    @patch('airflow.hooks.base_hook.BaseHook.get_connection')
    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    # @patch('observatory.dags.telescopes.google_books.GoogleBooksTelescope.DOWNLOAD_MODE', 'sequential')
    def test_download_reports(self, mock_variable_get, mock_connection_get):
        with CliRunner().isolated_filesystem():
            # set telescope data path variable
            mock_variable_get.return_value = 'data'
            mock_connection_get.side_effect = side_effect
            release = GoogleBooksRelease(self.start_date, self.end_date, self.account_number)
            download_reports(release)


def side_effect(arg):
    values = {
        'oaebu_service_account': 'unit_test@unit.test',
        'oaebu_password': 'passwd'
    }
    return values[arg]

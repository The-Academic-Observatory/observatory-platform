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

# Author: James Diprose

import unittest

import pendulum
from click.testing import CliRunner

from observatory.platform.utils.jinja2_utils import render_template, make_jinja2_filename, make_sql_jinja2_filename


class TestJinja2Utils(unittest.TestCase):
    def test_render_template(self):
        template_path = "query.sql.jinja2"
        template = "{# Test comment #}SELECT * FROM `{{ project_id }}.{{ dataset_id }}.Affiliations{{ release_date.strftime('%Y%m%d') }}` LIMIT 1000"
        expected_render = "SELECT * FROM `academic-observatory.mag.Affiliations20200810` LIMIT 1000"

        with CliRunner().isolated_filesystem():
            # Write test template
            with open(template_path, "w") as f:
                f.write(template)

            # Render template and test that we get expected render
            render = render_template(
                template_path,
                project_id="academic-observatory",
                dataset_id="mag",
                release_date=pendulum.datetime(year=2020, month=8, day=10),
            )
            self.assertEqual(render, expected_render)

    def test_make_jinja2_filename(self):
        input_file_name = "aggregate_crossref.sql"
        expected_file_name = "aggregate_crossref.sql.jinja2"
        actual_file_name = make_jinja2_filename(input_file_name)
        self.assertEqual(expected_file_name, actual_file_name)

    def test_make_sql_jinja2_filename(self):
        input_file_name = "aggregate_crossref"
        expected_file_name = "aggregate_crossref.sql.jinja2"
        actual_file_name = make_sql_jinja2_filename(input_file_name)
        self.assertEqual(expected_file_name, actual_file_name)

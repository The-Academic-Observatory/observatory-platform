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

import os.path
import tempfile
import unittest

import pendulum

from observatory_platform.jinja2_utils import make_jinja2_filename, make_sql_jinja2_filename, render_template


# Author: James Diprose


class TestJinja2Utils(unittest.TestCase):
    def test_render_template(self):
        template = "{# Test comment #}SELECT * FROM `{{ project_id }}.{{ dataset_id }}.Affiliations{{ snapshot_date.strftime('%Y%m%d') }}` LIMIT 1000"
        expected_render = "SELECT * FROM `academic-observatory.mag.Affiliations20200810` LIMIT 1000"

        with tempfile.TemporaryDirectory() as t:
            template_path = os.path.join(t, "query.sql.jinja2")

            # Write test template
            with open(template_path, "w") as f:
                f.write(template)

            # Render template and test that we get expected render
            render = render_template(
                template_path,
                project_id="academic-observatory",
                dataset_id="mag",
                snapshot_date=pendulum.datetime(year=2020, month=8, day=10),
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

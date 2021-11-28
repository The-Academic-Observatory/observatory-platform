# Copyright 2021 Curtin University
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

import os
import unittest

from click.testing import CliRunner

from observatory.platform.docker.builder import Builder, Template, rendered_file_name


class TestTemplate(unittest.TestCase):
    def test_rendered_file_name(self):
        template_file_path = os.path.join("path", "to", "template.yaml.jinja2")
        expected_output_filename = "template.yaml"
        actual_file_name = rendered_file_name(template_file_path)
        self.assertEqual(expected_output_filename, actual_file_name)

    def test_output_file_name(self):
        template_file_path = os.path.join("path", "to", "template.yaml.jinja2")
        t = Template(path=template_file_path, kwargs={})
        expected_output_filename = "template.yaml"
        self.assertEqual(expected_output_filename, t.output_file_name)


class TestBuilder(unittest.TestCase):
    def test_add_template(self):
        expected_path = os.path.join("path", "to", "template.yaml.jinja2")
        expected_kwargs = {"key1": 1, "key2": 2}

        builder = Builder(build_path="/")
        builder.add_template(path=expected_path, **expected_kwargs)

        self.assertEqual(1, len(builder.templates))
        template = builder.templates[0]
        self.assertEqual(expected_path, template.path)
        self.assertEqual(expected_kwargs, template.kwargs)

    def test_add_file(self):
        expected_path = os.path.join("path", "to", "my-file.txt")
        expected_output_file_name = "my-file-renamed.txt"

        builder = Builder(build_path="/")
        builder.add_file(path=expected_path, output_file_name=expected_output_file_name)

        self.assertEqual(1, len(builder.files))
        file = builder.files[0]
        self.assertEqual(expected_path, file.path)
        self.assertEqual(expected_output_file_name, file.output_file_name)

    def test_render_template(self):
        with CliRunner().isolated_filesystem() as t:
            # Make a template
            template_path = os.path.join(t, "template.txt.jinja2")
            with open(template_path, mode="w") as f:
                f.write("{{ key }}")
            expected_content = "hello"
            template = Template(path=template_path, kwargs={"key": expected_content})

            # Render the template
            builder = Builder(build_path=t)
            output_file_path = os.path.join(t, "template.txt")
            builder.render_template(template, output_file_path)

            # Check output
            with open(output_file_path, mode="r") as f:
                content = f.read()
            self.assertEqual(expected_content, content)

    def test_make_files(self):
        with CliRunner().isolated_filesystem() as t:
            build_path = os.path.join(t, "build")

            builder = Builder(build_path=build_path)
            file_path = os.path.join(t, "file.txt")
            output_file_name = "my-file.txt"
            template_path = os.path.join(t, "template.txt.jinja2")
            builder.add_file(path=file_path, output_file_name=output_file_name)
            expected_content = "hello"
            builder.add_template(path=template_path, key=expected_content)

            # Add files
            with open(file_path, mode="w") as f:
                f.write(expected_content)
            with open(template_path, mode="w") as f:
                f.write("{{ key }}")

            # Make files
            builder.make_files()

            # Check files and folders exist
            self.assertTrue(os.path.exists(build_path))

            expected_file_path = os.path.join(build_path, output_file_name)
            self.assertTrue(os.path.exists(expected_file_path))
            with open(expected_file_path, mode="r") as f:
                content = f.read()
            self.assertEqual(expected_content, content)

            expected_template_path = os.path.join(build_path, "template.txt")
            self.assertTrue(os.path.exists(expected_template_path))
            with open(expected_template_path, mode="r") as f:
                content = f.read()
            self.assertEqual(expected_content, content)

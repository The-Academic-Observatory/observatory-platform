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
#
# Author: Rebecca Handcock

import os
import json

def create_dataset_doc_md():
    """Utility function to create the Markdown files needed to display
    telescope details and schema on ReadTheDocs
    """

    # setup file to write out to
    filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "docs", "doc_telescopes.md"))
    f_docindex = open(filepath, "w")

    # Write file header
    f_docindex.write("# Telescope Datasets and Methods\n")
    f_docindex.write("- - -\n")

    # Get a sorted list of telescope JSONs
    json_sorted = []
    jsondir_filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "database", "analysis", "bigquery", "schema"))

    for subdir, dirs, files in os.walk(jsondir_filepath):
        for t_filename in files:
            if t_filename.endswith(".json"):
                json_sorted.append(t_filename)
        json_sorted = sorted(json_sorted)

    # Loop through just the jsons, creating documention files and entry in the index file
    # t_json is a list of type dict
    for t_json in json_sorted:
        t_json_name = t_json.split(".")[0]
        f_docindex.write("*  " + t_json_name + "\n")

        # Create the telescope documentation file

        # setup file to write out to
        filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "docs", "telescopes", t_json_name)) + ".md"
        t_docfile = open(filepath, "w")

        # If there is a template file for this telescope, add the text to the output file
        filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "docs", "templates", t_json_name)) + ".txt"
        if (os.path.exists(filepath)):
          t_templatefile = open(filepath, "r")
          for line in t_templatefile:
              t_docfile.write(line + "\n")
          t_templatefile.close()
        else:
          t_docfile.write("# Telescope: " + t_json_name + "\n")

        # Write out schema header
        t_docfile.write("- - - \n")
        t_docfile.write("## Dataset Schema\n\n")

        def json_print_leaf(dict2print: dict, dict_level: int):
            prefix = " " * 4 * (dict_level - 1)
            t_docfile.write(prefix + "+ **" + dict2print['name'] +
                            "** [*" + str.capitalize(dict2print['type']) + "*]")
            if ('mode' in dict2print):
                t_docfile.write(" " + str.capitalize(dict2print['mode']) + "\n")
            else:
                t_docfile.write("\n")


        def json_print_level_loop (json_list: list, level: int):
            for element in json_list:
              if (element['type'] != 'RECORD'):
                  json_print_leaf(element, level)
              else:
                  json_print_leaf(element, level)
                  next_level = level + 1
                  fields = element['fields']
                  json_print_level_loop(fields, next_level)
            return()


        # Write out schema from JSON
        json_filepath = os.path.join(subdir, t_json)
        t_jsonfile = open(json_filepath, "r")
        json_list = json.load(t_jsonfile)

        json_print_level_loop(json_list, 1)
        t_docfile.write("- - - \n")

        # tidy up files
        t_jsonfile.close()
        t_docfile.close()

    # tidy up
    f_docindex.close()
    return ()

create_dataset_doc_md()

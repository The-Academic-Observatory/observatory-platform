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

# Author: Tuan Chien


import json
import os
import re
import shutil
from glob import glob
from pathlib import Path
from typing import List

import pandas as pd


def schema_to_csv(*, schema: List, output: List, prefix: str = ""):
    """
    Convert a schema file to a pandas dataframe, ready for csv export.

    :param schema: Schema list.
    :param output: Output list of rows.
    :param prefix: Name prefixes for when there are nested records.
    """

    for field in schema:
        fname = field["name"] if "name" in field else None
        ftype = field["type"] if "type" in field else None
        fmode = field["mode"] if "mode" in field else None
        fdesc = field["description"] if "description" in field else None

        name = f"{prefix}{fname}"
        row = {"name": name, "type": ftype, "mode": fmode, "description": fdesc}
        output.append(row)

        if ftype == "RECORD":
            ffield = field["fields"]
            schema_to_csv(schema=ffield, output=output, prefix=f"{prefix}{fname}.")


def generate_csv(*, schema_dir):
    """Convert all observatory schema files in JSON format to CSV for inclusion in Sphinx.
    :param schema_dir: Path to schema directory.
    """

    schema_files = glob(os.path.join(schema_dir, "*.json"))
    dst_dir = "schemas"

    if os.path.exists(dst_dir):
        shutil.rmtree(dst_dir)

    Path(dst_dir).mkdir(exist_ok=True, parents=True)

    for schema_file in schema_files:
        filename = os.path.basename(schema_file)
        filename = filename[:-4] + "csv"  # Remove json and add csv suffix
        with open(schema_file, "r") as f:
            data = f.read()
        schema = json.loads(data)
        rows = list()
        schema_to_csv(schema=schema, output=rows)
        df = pd.DataFrame(rows)
        df.to_csv(os.path.join(dst_dir, filename), index=False)


def generate_latest_files():
    """For each schema, generate a schema_latest.csv to indicate it is the latest dataset schema. Then if people just
    want to refer to the latest schema in the documentation they can refer to this link rather than having to update
    the documentation when there is a schema change.

    Does not handle versioned schemas  (wos/scopus). But maybe we should get rid of versioned schemas when those
    telescopes are ported to the new template framework anyway.
    """

    table_files = glob("schemas/*.csv")
    r = re.compile(r"\d{4}-\d{2}-\d{2}")

    # Build a database of schema files
    table_schemas = {}
    for file in table_files:
        filename = os.path.basename(file)
        date_str = r.search(filename)
        date_str_start = date_str.span()[0]
        table_name = filename[: date_str_start - 1]  # -1 accounts for _
        table_schemas[table_name] = table_schemas.get(table_name, list())
        table_schemas[table_name].append(file)

    # Sort schemas
    for table in table_schemas:
        table_schemas[table].sort()

    # Copy the last schema in list since it's latest, to a table_latest.csv
    for table in table_schemas:
        dst_dir = "schemas"
        dst_filename = f"{table}_latest.csv"
        dst_path = os.path.join(dst_dir, dst_filename)
        src_file = table_schemas[table][-1]
        shutil.copyfile(src_file, dst_path)

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

# Author: Tuan Chien


import json
import os
from glob import glob
from pathlib import Path
from typing import Dict, List, Union

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


if __name__ == "__main__":
    generate_csv(schema_dir="../observatory-dags/observatory/dags/database/schema")

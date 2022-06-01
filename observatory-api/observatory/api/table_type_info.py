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
#
# Author: Tuan Chien

from collections import OrderedDict
from observatory.api.client.model.table_type import TableType
from observatory.api.utils import get_api_client, seed_table_type


def get_table_type_info():
    """Get a dictionary of table type info.
    :param api: 
    """

    table_type_info = OrderedDict()

    table_type_info["regular"] = TableType(type_id="regular", name="Regular BigQuery table")
    table_type_info["sharded"] = TableType(type_id="sharded", name="Sharded BigQuery table")
    table_type_info["partitioned"] = TableType(type_id="partitioned", name="Partitioned BigQuery table")

    return table_type_info


if __name__ == "__main__":
    api = get_api_client()
    table_type_info = get_table_type_info()
    seed_table_type(api=api, table_type_info=table_type_info)
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

# Author: Aniek Roelofs

from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1)
}

with DAG(dag_id="sync_github_repo", schedule_interval="@hourly", default_args=default_args, catchup=False) as dag:
    task_sync = BashOperator(
        task_id="sync_github_repo",
        # Get bucket url of dags folder for this composer environment.
        # Sync the content of the 'github' bucket to the dags folder. The 'github' bucket is updated upon creation
        # of the composer environment and upon any pushes from the develop/master branch.
        # GITHUB_BUCKET_PATH and is using terraform.
        bash_command="export DAGS_BUCKET_PATH=$(gcloud composer environments describe $COMPOSER_ENVIRONMENT_NAME "
                     "--location $COMPOSER_LOCATION --format='get(config.dagGcsPrefix)'); "
                     "gsutil -m rsync -r -c $GITHUB_BUCKET_PATH $DAGS_BUCKET_PATH"
    )
    task_sync

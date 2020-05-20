"""
Parses the xml response containing info on unpaywall releases.
Based on the returned info it checks whether there is an existing bigquery table with the same release.
If the table doesn't exist yet, it will trigger a separate dag 'unpaywall_target.py'.
"""
import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from release_util import UnpaywallRelease as ReleaseUtil
from trigger_util import create_trigger_dag

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 5, 1)
}

data_source='unpaywall'
#TODO don't hardcode these variables, not sure if they should be coming from Terraform
project_id = 'workflows-dev'
bucket = 'workflows-dev-910922156021-test'

variable_no_snapshots = f"{data_source}_no_snapshots"
variable_snapshot_url_list = f"{data_source}_snapshot_url_list"
target_dag_id = f"{data_source}_target"
#TODO get from environment, set e.g. to 'production' when using terraform.
testing_environment = 'dev'


def create_url_debug():
    """
    Adds one url to snapshot_list of a small subset which is hosted on gcp in a storage bucket.
    """
    snapshot_list = ''

    snapshot_list += 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/unpaywall_snapshot_3000-01-27T153236.jsonl.gz' + ', '
    Variable.set(variable_snapshot_url_list, snapshot_list)
    Variable.set(variable_no_snapshots, 1)


def create_url_list():
    """
    Takes an url that gives an xml response containing info on releases.
    Sets 2 Airflow variables based on this response.

    The xml response is parsed and the names of the releases are stored in a ', ' separated string.
    This string is set as an Airflow variable. Airflow variables only can be retrieved as strings (e.g. not list),
    the string can be easily transformed in a list later on.

    The number of releases available is also set as a variable which is used to loop through the list.
    """
    snapshot_list = ''
    no_snapshots = 0
    xml_string = requests.get(ReleaseUtil.host).text
    bq_hook = BigQueryHook()

    if xml_string:
        # parse xml file and get list of snapshots
        root = ET.fromstring(xml_string)
        for unpaywall_release in root.findall('.//{http://s3.amazonaws.com/doc/2006-03-01/}Key'):
            snapshot_url = os.path.join(ReleaseUtil.host, unpaywall_release.text)
            snapshot_release = ReleaseUtil(snapshot_url)
            table_exists = bq_hook.table_exists(
                project_id=project_id,
                dataset_id=data_source,
                table_id=snapshot_release.friendly_name
            )
            if not table_exists:
                snapshot_list += snapshot_url + ', '
                no_snapshots += 1
    Variable.set(variable_snapshot_url_list, snapshot_list)
    Variable.set(variable_no_snapshots, no_snapshots)


dag_id = create_trigger_dag(bucket, data_source, default_args, create_url_debug, create_url_list,
                            ReleaseUtil, testing_environment=testing_environment)
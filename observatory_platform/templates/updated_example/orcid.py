from observatory_platform.templates.updated_example.stream_telescope import StreamTelescope, StreamRelease
import pendulum
from typing import Union, List
from observatory_platform.utils.config_utils import SubFolder
from datetime import datetime
from datetime import timedelta
import logging
from airflow.models.taskinstance import TaskInstance

class OrcidRelease(StreamRelease):
    def __init__(self, dag_id: str, start_date: Union[pendulum.Pendulum, None], end_date: pendulum.Pendulum,
                 first_release: bool = False):
        super().__init__(dag_id, start_date, end_date, first_release)
        self.lambda_path = self.create_path(SubFolder.downloaded, "lambda_path", "txt")

    @staticmethod
    def make_release(telescope, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum,
                     first_release: bool) -> 'OrcidRelease':
        release = OrcidRelease(telescope.dag_id, start_date, end_date, first_release)
        return release


def transfer_release(release: OrcidRelease, **kwargs):
    print(release)
    print('download release', release.lambda_path, release.blob_dir)


def download_from_bucket(release: OrcidRelease, **kwargs):
    pass


def extract(release: OrcidRelease, **kwargs):
    pass


def transform(release: OrcidRelease, **kwargs):
    pass


def test_callable(something_else: str, **kwargs):
    pass


def before_subdag(**kwargs):
    pass

# airflow DAG
orcid_subdags = StreamTelescope(release_cls=OrcidRelease, dag_id='orcid_subdags', subdag_ids=['subdag1', 'subdag2',
                                                                                              'subdag3'],
                                queue='default', schedule_interval='@weekly',
                                catchup=False, start_date=datetime(2020, 11, 1), max_retries=3, description='',
                                dataset_id='dataset_id', schema_version='', airflow_vars=[],
                                airflow_conns=[], transform_filenames=['orcid_table1', 'orcid_table2'],
                                merge_partition_fields=['merge_field1', 'merge_field2'], updated_date_fields=[
        'update_field1', 'update_field2'],
                                bq_merge_days=1)

orcid_subdags.add_before_subdag_chain(before_subdag)
orcid_subdags.add_extract_chain([test_callable, transfer_release, download_from_bucket, extract], ['subdag1'])
orcid_subdags.add_transform_chain(transform, orcid_subdags.subdag_ids, append=False)

globals()[orcid_subdags.dag_id] = orcid_subdags.make_dags()

orcid = StreamTelescope(release_cls=OrcidRelease, dag_id='orcid_singledag', subdag_ids=[],
                                queue='default', schedule_interval='@weekly',
                                catchup=False, start_date=datetime(2020, 11, 1), max_retries=3, description='',
                                dataset_id='dataset_id', schema_version='', airflow_vars=[],
                                airflow_conns=[], transform_filenames=['orcid_table1'],
                                merge_partition_fields=['merge_field1'], updated_date_fields=['update_field1'],
                                bq_merge_days=1)

orcid.add_extract_chain([test_callable, transfer_release, download_from_bucket, extract])
orcid.add_transform_chain(transform, append=False)

globals()[orcid.dag_id] = orcid.make_dags()



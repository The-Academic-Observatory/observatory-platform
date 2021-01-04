from observatory_platform.templates.updated_example.stream_telescope import StreamTelescope, StreamRelease
import pendulum
from typing import Union
from observatory_platform.utils.config_utils import SubFolder
from datetime import datetime
from airflow.models.taskinstance import TaskInstance


class OrcidRelease(StreamRelease):
    def __init__(self, dag_id: str, start_date: Union[pendulum.DateTime, None], end_date: pendulum.DateTime,
                 first_release: bool = False):
        super().__init__(dag_id, start_date, end_date, first_release)
        self.lambda_path = self.create_path(SubFolder.downloaded, "lambda_path", "txt")

    @staticmethod
    def make_release(**kwargs) -> 'OrcidRelease':
        # Make Release instance
        ti: TaskInstance = kwargs['ti']
        start_date, end_date, first_release = ti.xcom_pull(key=orcid.RELEASE_INFO, include_prior_dates=True)

        release = OrcidRelease(orcid.dag_id, start_date, end_date, first_release)
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
orcid_subdags = StreamTelescope(release_cls=OrcidRelease, dag_id='orcid_subdags',
                                subdag_ids=['subdag1', 'subdag2', 'subdag3'], queue='default',
                                schedule_interval='@weekly', catchup=False, start_date=datetime(2020, 11, 1),
                                max_retries=3, description='', dataset_id='dataset_id', schema_version='',
                                airflow_vars=[], airflow_conns=[], transform_filenames=['orcid_table1', 'orcid_table2'],
                                merge_partition_fields=['merge_field1', 'merge_field2'],
                                updated_date_fields=['update_field1', 'update_field2'], bq_merge_days=1)

orcid_subdags.add_setup_chain([before_subdag, orcid_subdags.get_release_info])
orcid_subdags.add_extract_chain([test_callable, transfer_release, download_from_bucket, extract])
orcid_subdags.add_transform_task(transform)

globals()[orcid_subdags.dag_id] = orcid_subdags.make_dag()

orcid = StreamTelescope(release_cls=OrcidRelease, dag_id='orcid_singledag', subdag_ids=[], queue='default',
                        schedule_interval='@weekly', catchup=False, start_date=datetime(2020, 11, 1), max_retries=3,
                        description='', dataset_id='dataset_id', schema_version='', airflow_vars=[], airflow_conns=[

    ], transform_filenames=['orcid_table1'], merge_partition_fields=['merge_field1'],
                        updated_date_fields=['update_field1'], bq_merge_days=1)

orcid.add_setup_task(orcid.get_release_info)
orcid.add_extract_chain([test_callable, transfer_release, download_from_bucket, extract])
orcid.add_transform_chain([transform, orcid.upload_transformed_task])
orcid.add_load_chain(orcid.make_operators([orcid.bq_load_partition_task, orcid.bq_delete_old_task, orcid.bq_append_new_task]))
orcid.add_cleanup_task(orcid.cleanup)

globals()[orcid.dag_id] = orcid.make_dag()

from observatory_platform.templates.updated_example.stream_telescope import StreamTelescope
from observatory_platform.templates.updated_example.telescope import TelescopeRelease
import pendulum
from typing import Union
from observatory_platform.utils.config_utils import SubFolder
from datetime import datetime
from observatory_platform.utils.telescope_utils import upload_downloaded


class OrcidRelease(TelescopeRelease):
    def __init__(self, dag_id: str, start_date: Union[pendulum.Pendulum, None], end_date: pendulum.Pendulum,
                 extensions: dict, first_release: bool = False):
        super().__init__(dag_id, start_date, end_date, extensions, first_release)
        self.lambda_path = self.get_path(SubFolder.downloaded, "lambda_path", "txt")


class OrcidTelescope(StreamTelescope):
    def check_dependencies(self, **kwargs):
        super().check_dependencies(**kwargs)

    def make_release(self, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, first_release: bool) -> \
            OrcidRelease:
        release = OrcidRelease(self.dag_id, start_date, end_date, self.extensions, first_release)
        return release

    def download_pull(self, release: OrcidRelease, **kwargs) -> bool:
        print(release)
        print('download release', release.lambda_path, release.download_path)
        return True

    def download_push(self, release: OrcidRelease, **kwargs):
        upload_downloaded(release.download_dir, release.blob_dir)

    def extract(self, release: TelescopeRelease, **kwargs):
        pass

    def transform(self, release: TelescopeRelease, **kwargs):
        pass


# airflow DAG
orcid = OrcidTelescope(dag_id='orcid_test', queue='default', schedule_interval='@weekly',
                       start_date=datetime(2020, 11, 1), max_retries=3, description='', download_pull_first=True,
                       dataset_id='dataset_id', schema_version='', airflow_vars=[], airflow_conns=[],
                       download_ext='json', extract_ext='json', transform_ext='jsonl', main_table_id='main_table_id',
                       partition_table_id='partition_table_id', merge_partition_field='merge_field',
                       updated_date_field='update_field', bq_merge_days=1)

globals()[orcid.dag_id] = orcid.make_dag()

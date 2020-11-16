from observatory_platform.templates.updated_example.specialised_telescope import SpecialisedTelescope, SpecialisedRelease
from datetime import datetime
import pendulum


# class GridRelease(SpecialisedRelease):
#     """ Used to store info on a given release. """
#     def __init__(self, dag_id: str, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum,
#                  extensions: dict, extra_param: str, first_release: bool = False):
#         super().__init__(dag_id, start_date, end_date, extensions, first_release)
#         self.extra_param = extra_param


class GridTelescope(SpecialisedTelescope):
    """ Telescope description """
    def __init__(self, dag_id, start_date, schedule_interval, extensions, partition_table_id):
        super().__init__(dag_id, start_date, schedule_interval, extensions, partition_table_id)

    # def create_release(self, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, first_release: bool):
    #     extra_param = 'extra'
    #     release = GridRelease(self.dag_id, start_date, end_date, self.extensions, extra_param, first_release)
    #     return release

    def download(self, release) -> bool:
        print('test from grid', self.dag_id)
        print('download path & directory is created', release.download_path)
        return True


grid = GridTelescope(dag_id='dag_id', start_date=datetime(2020, 11, 1), schedule_interval='@weekly',
                     extensions={'download': 'json', 'extract': 'json', 'transform': 'jsonl'},
                     partition_table_id='test_id')
globals()[grid.dag_id] = grid.create_airflow_DAG()

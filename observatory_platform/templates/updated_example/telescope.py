import os
import pendulum
from typing import Union
from observatory_platform.utils.config_utils import SubFolder
from observatory_platform.utils.config_utils import telescope_path


class TelescopeRelease:
    """ Used to store info on a given release"""
    def __init__(self, dag_id: str, start_date: Union[pendulum.Pendulum, None], end_date: pendulum.Pendulum,
                 extensions: dict, first_release: bool = False):
        self.dag_id = dag_id
        self.start_date = start_date
        self.end_date = end_date
        self.release_date = end_date
        self.extensions = extensions
        self.first_release = first_release
        self.download_ext = extensions['download']
        self.extract_ext = extensions['extract']
        self.transform_ext = extensions['transform']

    @property
    def date_str(self) -> str:
        return self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")
        # super().__init__(dag_id, start_date, end_date, extensions, first_release)

    @property
    def file_name(self) -> str:
        return f"{self.dag_id}_{self.date_str}"

    @property
    def blob_dir(self) -> str:
        return f'telescopes/{self.dag_id}'

    @property
    def download_blob(self) -> str:
        return os.path.join(self.blob_dir, f"{self.file_name}.{self.download_ext}")

    @property
    def download_path(self) -> str:
        return self.get_path(SubFolder.downloaded, self.file_name, self.download_ext)

    @property
    def download_dir(self) -> str:
        return self.subdir(SubFolder.downloaded)

    @property
    def extract_path(self) -> str:
        return self.get_path(SubFolder.extracted, self.file_name, self.extract_ext)

    @property
    def extract_dir(self) -> str:
        return self.subdir(SubFolder.extracted)

    @property
    def transform_blob(self) -> str:
        return os.path.join(self.blob_dir, f"{self.file_name}.{self.transform_ext}")

    @property
    def transform_path(self) -> str:
        return self.get_path(SubFolder.transformed, self.file_name, self.transform_ext)

    @property
    def transform_dir(self) -> str:
        return self.subdir(SubFolder.transformed)

    def subdir(self, sub_folder: SubFolder) -> str:
        """ Path to subdirectory of a specific release for either downloaded/extracted/transformed files.
        Will also create the directory if it doesn't exist yet.
        :param sub_folder: Name of the subfolder
        :return: Path to the directory
        """
        subdir = os.path.join(telescope_path(sub_folder, self.dag_id), self.date_str)
        if not os.path.exists(subdir):
            os.makedirs(subdir, exist_ok=True)
        return subdir

    def get_path(self, sub_folder: SubFolder, file_name: str, ext: str) -> str:
        """
        Gets path to file based on subfolder, file name and extension.
        :param sub_folder: Name of the subfolder
        :param file_name: Name of the file
        :param ext: The extension of the file
        :return: The file path.
        """
        path = os.path.join(self.subdir(sub_folder), f"{file_name}.{ext}")
        return path

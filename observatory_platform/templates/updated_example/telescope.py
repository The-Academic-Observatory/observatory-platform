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
        self.download_ext = extensions['download']
        self.extract_ext = extensions['extract']
        self.transform_ext = extensions['transform']
        self.first_release = first_release

        self.date_str = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")

        # paths
        self.file_name = f"{self.dag_id}_{self.date_str}"
        self.blob_dir = f'telescopes/{self.dag_id}'

        self.download_blob = os.path.join(self.blob_dir, f"{self.file_name}.{self.download_ext}")
        self.download_path = self.get_path(SubFolder.downloaded, self.file_name, self.download_ext)
        self.download_dir = self.subdir(SubFolder.downloaded)

        self.extract_path = self.get_path(SubFolder.extracted, self.file_name, self.extract_ext)
        self.extract_dir = self.subdir(SubFolder.extracted)

        self.transform_blob = os.path.join(self.blob_dir, f"{self.file_name}.{self.transform_ext}")
        self.transform_path = self.get_path(SubFolder.transformed, self.file_name, self.transform_ext)
        self.transform_dir = self.subdir(SubFolder.transformed)

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

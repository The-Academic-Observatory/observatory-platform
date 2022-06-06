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


from pathlib import Path
from typing import List
from glob import glob
import os
from dataclasses import dataclass
import shutil


@dataclass
class SeedFile:
    """API seed file."""

    prefix: str
    path: str


def copy_api_seed_files(*, build_path: str, pkgs: List["PythonPackage"]):  # noqa: F821
    """Copy the api seed files into the build directory.

    :param build_path: Observatory platform docker build path.
    :param pkgs: Observatory packages metadata.
    """

    seed_dir = create_dir(build_path)
    seed_files = find_seeding_files(pkgs)
    copy_files(seed_dir=seed_dir, seed_files=seed_files)


def create_dir(build_path: str) -> str:
    """Create the directory in build path to copy seed files to.

    :param build_path: Observatory platform docker build path.
    :return: Seed file directory.
    """

    dst_dir = os.path.join(build_path, "seed_db")
    Path(dst_dir).mkdir(parents=True, exist_ok=True)
    return dst_dir


def find_seeding_files(pkgs: List["PythonPackages"]) -> List[SeedFile]:  # noqa: F821
    """Find the api db seed scripts.

    :param pkgs: Observatory Python packages to search.
    :return: List of seed files found.
    """

    search_files = [
        "table_type_info.py",
        "dataset_type_info.py",
        "workflow_type_info.py",
        "organisation_info.py",
        "workflow_info.py",
        "dataset_info.py",
    ]

    seed_files = list()
    for pkg in pkgs:
        for file in search_files:
            search_pattern = os.path.join(pkg.host_package, "**", file)
            found = glob(search_pattern, recursive=True)
            for path in found:
                seed_file = SeedFile(prefix=pkg.name, path=path)
                seed_files.append(seed_file)

    return seed_files


def copy_files(seed_dir: str, seed_files: List[SeedFile]):
    """Copy seed files into build directory.

    :param seed_dir: Target directory.
    :param seed_files: Files to copy.
    """

    for src in seed_files:
        prefix = src.prefix
        filename = os.path.basename(src.path)
        dst_file = f"{prefix}.{filename}"
        dst_path = os.path.join(seed_dir, dst_file)
        shutil.copyfile(src.path, dst_path)

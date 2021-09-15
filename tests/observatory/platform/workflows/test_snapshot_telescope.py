# Copyright 2021 Curtin University
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

from unittest.mock import MagicMock, patch

import pendulum
from observatory.platform.utils.test_utils import ObservatoryTestCase
from observatory.platform.workflows.snapshot_telescope import (
    SnapshotRelease,
    SnapshotTelescope,
)
from observatory.platform.workflows.workflow import Release


class MockTelescope(SnapshotTelescope):
    def __init__(self):
        super().__init__(
            dag_id="dag",
            start_date=pendulum.now(),
            schedule_interval="@monthly",
            dataset_id="data",
            schema_folder="folder",
        )

    def make_release(self, **kwargs):
        return [SnapshotRelease(dag_id="dag", release_date=pendulum.now())]


class TestSnapshotTelescope(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_download(self, m_get):
        m_get.return_value = "data"
        telescope = MockTelescope()
        releases = telescope.make_release()
        releases[0].download = MagicMock()
        telescope.download(releases)
        self.assertEqual(releases[0].download.call_count, 1)

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_upload_downloaded(self, m_get):
        m_get.return_value = "data"
        with patch("observatory.platform.workflows.snapshot_telescope.upload_files_from_list") as m_upload:
            telescope = MockTelescope()

            releases = telescope.make_release()
            telescope.upload_downloaded(releases)

            self.assertEqual(m_upload.call_count, 1)
            call_args, _ = m_upload.call_args
            self.assertEqual(call_args[0], [])
            self.assertEqual(call_args[1], "data")

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_extract(self, m_get):
        m_get.return_value = "data"
        telescope = MockTelescope()
        releases = telescope.make_release()
        releases[0].extract = MagicMock()
        telescope.extract(releases)
        self.assertEqual(releases[0].extract.call_count, 1)

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_transform(self, m_get):
        m_get.return_value = "data"
        telescope = MockTelescope()
        releases = telescope.make_release()
        releases[0].transform = MagicMock()
        telescope.transform(releases)
        self.assertEqual(releases[0].transform.call_count, 1)

# Copyright 2025 Curtin University
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

# Author: Keegan Smith

import base64
import unittest
from unittest.mock import Mock, patch

from oaebu_workflows.muse_telescope.muse_telescope import (
    gmail_add_label_tomessage,
    gmail_create_service,
    gmail_download_attachment,
    gmail_get_message_attachment_ids,
    gmail_get_messages,
)


class TestGmailFunctions(unittest.TestCase):
    def setUp(self):
        self.mock_service = Mock()

    def test_gmail_create_service(self):
        with (
            patch("oaebu_workflows.muse_telescope.muse_telescope.BaseHook") as mock_hook,
            patch("oaebu_workflows.muse_telescope.muse_telescope.Credentials") as mock_creds,
            patch("oaebu_workflows.muse_telescope.muse_telescope.build") as mock_build,
        ):
            mock_conn = Mock()
            mock_conn.extra_dejson = {}
            mock_hook.get_connection.return_value = mock_conn
            mock_creds.from_authorized_user_info.return_value = "dummy_creds"
            mock_build.return_value = self.mock_service

            service = gmail_create_service()

            self.assertEqual(service, self.mock_service)
            mock_hook.get_connection.assert_called_once_with("gmail_api")
            mock_creds.from_authorized_user_info.assert_called_once()
            mock_build.assert_called_once_with("gmail", "v1", credentials="dummy_creds", cache_discovery=False)

    def test_gmail_download_attachment(self):
        self.mock_service.users.return_value.messages.return_value.attachments.return_value.get.return_value.execute.return_value = {
            "data": base64.urlsafe_b64encode(b"test_data").decode("utf-8")
        }
        with patch("builtins.open", unittest.mock.mock_open()) as mock_file:
            gmail_download_attachment(self.mock_service, "msg_id", "att_id", "dummy_path")

            mock_file.assert_called_once_with("dummy_path", "wb")
            mock_file().write.assert_called_once_with(b"test_data")

    def test_gmail_get_messages(self):
        self.mock_service.users.return_value.messages.return_value.list.return_value.execute.side_effect = [
            {"messages": [{"id": 1}], "nextPageToken": "token"},
            {"messages": [{"id": 2}]},
        ]

        messages = gmail_get_messages(self.mock_service, {})

        self.assertEqual(messages, [{"id": 1}, {"id": 2}])

    def test_gmail_get_message_attachment_ids(self):
        self.mock_service.users.return_value.messages.return_value.get.return_value.execute.return_value = {
            "payload": {
                "parts": [
                    {"filename": "file1.csv", "body": {"attachmentId": "att_id_1"}},
                    {"filename": "", "body": {}},  # Not an attachment
                    {"filename": "file2.csv", "body": {"attachmentId": "att_id_2"}},
                ]
            }
        }

        ids = gmail_get_message_attachment_ids(self.mock_service, "msg_id")

        self.assertEqual(ids, ["att_id_1", "att_id_2"])

    def test_add_label_success(self):
        self.mock_service.users.return_value.messages.return_value.modify.return_value.execute.return_value = {
            "id": "msg_id",
            "labelIds": ["label_id"],
        }

        success = gmail_add_label_tomessage(self.mock_service, "msg_id", "label_id")

        self.assertTrue(success)

    def test_add_label_failure(self):
        self.mock_service.users.return_value.messages.return_value.modify.return_value.execute.return_value = {}

        success = gmail_add_label_tomessage(self.mock_service, "msg_id", "label_id")

        self.assertFalse(success)

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

import unittest
from unittest.mock import patch, MagicMock, mock_open
import base64
from googleapiclient.errors import HttpError

from observatory_platform.google.gmail import (
    gmail_create_service,
    gmail_download_attachment,
    gmail_get_messages,
    gmail_get_message_attachment_ids,
    gmail_add_label_to_message,
    gmail_get_label_id,
)


class TestGmailFunctions(unittest.TestCase):

    # ------------------------
    # gmail_create_service
    # ------------------------
    @patch("observatory_platform.google.gmail.build")
    @patch("observatory_platform.google.gmail.Credentials.from_authorized_user_info")
    def test_gmail_create_service(self, mock_creds, mock_build):
        info = {"token": "abc"}
        scopes = ["scope1"]
        mock_creds.return_value = "creds"
        mock_build.return_value = "service"

        service = gmail_create_service(info, scopes=scopes)
        mock_creds.assert_called_once_with(info, scopes=scopes)
        mock_build.assert_called_once_with("gmail", "v1", credentials="creds", cache_discovery=False)
        self.assertEqual(service, "service")

    # ------------------------
    # gmail_download_attachment
    # ------------------------
    @patch("observatory_platform.google.gmail.open", new_callable=mock_open)
    def test_gmail_download_attachment_success(self, mock_file):
        service = MagicMock()
        message_id = "m123"
        attachment_id = "a123"
        data_bytes = b"hello"
        encoded_data = base64.urlsafe_b64encode(data_bytes).decode("utf-8")
        service.users.return_value.messages.return_value.attachments.return_value.get.return_value.execute.return_value = {
            "data": encoded_data
        }

        gmail_download_attachment(service, message_id, attachment_id, "path/to/file")
        mock_file().write.assert_called_once_with(data_bytes)

    def test_gmail_download_attachment_no_data_raises(self):
        service = MagicMock()
        service.users.return_value.messages.return_value.attachments.return_value.get.return_value.execute.return_value = {
            "data": ""
        }
        with self.assertRaises(ValueError):
            gmail_download_attachment(service, "m123", "a123", "path")

    def test_gmail_download_attachment_http_error_404(self):
        service = MagicMock()
        error = HttpError(resp=MagicMock(status=404), content=b"")
        service.users.return_value.messages.return_value.attachments.return_value.get.return_value.execute.side_effect = (
            error
        )
        with self.assertRaises(HttpError):
            gmail_download_attachment(service, "m123", "a123", "path")

    # ------------------------
    # gmail_get_messages
    # ------------------------
    def test_gmail_get_messages_multiple_pages(self):
        service = MagicMock()
        first_result = {"messages": [{"id": "1"}], "nextPageToken": "token"}
        second_result = {"messages": [{"id": "2"}]}
        service.users.return_value.messages.return_value.list.return_value.execute.side_effect = [
            first_result,
            second_result,
        ]

        list_params = {"labelIds": ["INBOX"]}
        messages = gmail_get_messages(service, list_params)
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0]["id"], "1")
        self.assertEqual(messages[1]["id"], "2")

    def test_gmail_get_messages_no_messages(self):
        service = MagicMock()
        service.users.return_value.messages.return_value.list.return_value.execute.return_value = {}
        with patch("observatory_platform.google.gmail.logging.warning") as mock_log:
            messages = gmail_get_messages(service, {})
            self.assertEqual(messages, [])
            mock_log.assert_called_once()

    # ------------------------
    # gmail_get_message_attachment_ids
    # ------------------------
    def test_gmail_get_message_attachment_ids(self):
        service = MagicMock()
        msg_detail = {
            "payload": {
                "parts": [
                    {"filename": "file.txt", "body": {"attachmentId": "a1"}},
                    {"filename": "", "body": {"attachmentId": "a2"}},
                ]
            }
        }
        service.users.return_value.messages.return_value.get.return_value.execute.return_value = msg_detail
        attachment_ids = gmail_get_message_attachment_ids(service, "msg123")
        self.assertEqual(attachment_ids, ["a1"])

    def test_gmail_get_message_attachment_ids_404(self):
        service = MagicMock()
        error = HttpError(resp=MagicMock(status=404), content=b"")
        service.users.return_value.messages.return_value.get.return_value.execute.side_effect = error
        with self.assertRaises(HttpError):
            gmail_get_message_attachment_ids(service, "msg123")

    # ------------------------
    # gmail_add_label_to_message
    # ------------------------
    def test_gmail_add_label_to_message_success(self):
        service = MagicMock()
        service.users.return_value.messages.return_value.modify.return_value.execute.return_value = {
            "labelIds": ["label123"]
        }
        result = gmail_add_label_to_message(service, "msg123", "label123")
        self.assertTrue(result)

    def test_gmail_add_label_to_message_failure(self):
        service = MagicMock()
        service.users.return_value.messages.return_value.modify.return_value.execute.return_value = {"labelIds": []}
        result = gmail_add_label_to_message(service, "msg123", "label123")
        self.assertFalse(result)

    def test_gmail_add_label_to_message_404(self):
        service = MagicMock()
        error = HttpError(resp=MagicMock(status=404), content=b"")
        service.users.return_value.messages.return_value.modify.return_value.execute.side_effect = error
        with self.assertRaises(HttpError):
            gmail_add_label_to_message(service, "msg123", "label123")

    # ------------------------
    # gmail_get_label_id
    # ------------------------
    def test_gmail_get_label_id_existing(self):
        service = MagicMock()
        service.users.return_value.labels.return_value.list.return_value.execute.return_value = {
            "labels": [{"id": "id123", "name": "Test"}]
        }
        label_id = gmail_get_label_id(service, "Test")
        self.assertEqual(label_id, "id123")

    def test_gmail_get_label_id_create(self):
        service = MagicMock()
        service.users.return_value.labels.return_value.list.return_value.execute.return_value = {"labels": []}
        service.users.return_value.labels.return_value.create.return_value.execute.return_value = {"id": "new_id"}
        label_id = gmail_get_label_id(service, "NewLabel")
        self.assertEqual(label_id, "new_id")


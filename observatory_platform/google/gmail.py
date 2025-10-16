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

import logging
from typing import List, Sequence, Mapping
import base64

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import Resource, build
from googleapiclient.http import HttpError


def gmail_create_service(
    info: Mapping[str, str],
    scopes: Sequence[str] = (
        "https://www.googleapis.com/auth/gmail.readonly",
        "https://www.googleapis.com/auth/gmail.modify",
    ),
) -> Resource:
    """Build the gmail service.

    Reference API: https://developers.google.com/workspace/gmail/api/reference/rest/v1

    :param info: The autorhized user info in Google format
    :param scopes: The list of scopes to include in the credentials
    :return: Gmail service resource
    """
    creds = Credentials.from_authorized_user_info(info, scopes=scopes)
    service = build("gmail", "v1", credentials=creds, cache_discovery=False)
    return service


def gmail_download_attachment(service: Resource, message_id: str, attachment_id: str, download_path: str) -> None:
    """Download an attachment from a Gmail message.

    :param service: Gmail service resource
    :param message_id: The ID of the Gmail message that contains the attachment
    :param attachment_id: The ID of the attachment to download
    :param download_path: Path to download the attachment to
    """
    logging.info(f"Downloading attachment from mesage: '{message_id}' to: '{download_path}'")
    try:
        attachment = (
            service.users().messages().attachments().get(userId="me", messageId=message_id, id=attachment_id).execute()
        )
        data = attachment["data"]
        if not data:
            raise ValueError(f"Attachment '{attachment_id}' has no data.")
        file_data = base64.urlsafe_b64decode(data)
        with open(download_path, "wb") as f:
            f.write(file_data)
    except HttpError as e:
        if e.resp.status == 404:
            logging.error(f"Attachment '{attachment_id}' not found for message: '{message_id}'")
        else:
            logging.error(f"Gmail API error: {e}")
        raise


def gmail_get_messages(service: Resource, list_params: dict) -> List[dict]:
    """Get messages from the Gmail API.

    :param service: Gmail service resource
    :param list_params: The parameters that will be passed to the Gmail API.
    :return: List of messages. Each message is a dictionary.
    """
    first_query = True
    next_page_token = None
    messages = []
    while next_page_token or first_query:
        # Set the next page token if it isn't the first query
        if not first_query:
            list_params["pageToken"] = next_page_token
        first_query = False

        # Get the results
        results = service.users().messages().list(**list_params).execute()
        next_page_token = results.get("nextPageToken")
        if results.get("messages"):
            messages += results["messages"]

    if len(messages) == 0:
        logging.warning("No messages found for given parameters")
    return messages


def gmail_get_message_attachment_ids(service: Resource, message_id: str) -> List[str]:
    """Get attachment IDs from a message

    :param service: The Gmail service client
    :param message_id: The message ID to get the attachments of
    :return: All attachment IDs from the message
    """
    try:
        msg_detail = service.users().messages().get(userId="me", id=message_id).execute()
    except HttpError as e:
        if e.resp.status == 404:
            logging.error(f"Message '{message_id}' not found.")
        else:
            logging.error(f"Gmail API error: {e}")
        raise
    parts = msg_detail.get("payload", {}).get("parts", [])
    ids = []
    for part in parts:
        filename = part.get("filename")
        body = part.get("body", {})
        if filename:  # Only parts with a filename are attachments
            ids.append(body["attachmentId"])

    return ids


def gmail_add_label_to_message(
    service: Resource,
    message_id: str,
    label_id: str,
) -> bool:
    """Adds a label name to a gmail message.

    :param service: The Gmail service resource
    :param message_id: The ID of the message to add the label to
    :param label_id: The id of the label to add the message
    :return: True if successful, False otherwise
    """
    body = {"addLabelIds": label_id}
    try:
        response = service.users().messages().modify(userId="me", id=message_id, body=body).execute()
    except HttpError as e:
        if e.resp.status == 404:
            logging.error(f"Message '{message_id}' not found.")
        else:
            logging.error(f"Gmail API error: {e}")
        raise
    if response and label_id in response.get("labelIds", []):
        logging.info(f"Added label_id '{label_id}' to GMAIL message, message_id: {message_id}")
        return True

    logging.warning(f"Could not add label_id '{label_id}' to GMAIL message, message_id: {message_id}")
    return False


def gmail_get_label_id(service: Resource, label_name: str) -> str:
    """Get the id of a label based on the label name.

    :param label_name: The name of the label
    :return: The label id
    """
    existing_labels = service.users().labels().list(userId="me").execute()["labels"]
    label_id = [label["id"] for label in existing_labels if label["name"] == label_name]
    if label_id:
        label_id = label_id[0]
    else:
        # create label - it doesn't exist
        label_body = {
            "name": label_name,
            "messageListVisibility": "show",
            "labelListVisibility": "labelShow",
            "type": "user",
        }
        result = service.users().labels().create(userId="me", body=label_body).execute()
        label_id = result["id"]
    return label_id

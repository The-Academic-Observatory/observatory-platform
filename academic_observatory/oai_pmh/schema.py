#
# Copyright 2019 Curtin University. All rights reserved.
#
# Author: James Diprose
#

import datetime
from typing import Union, List

from academic_observatory.utils import unique_id


class Endpoint:
    def __init__(self, source_ref: any, source_url: str, admin_email: str, author: str, base_url: str,
                 comment: str, compression: str, content: str, creator: str, data_policy: str, dc: str,
                 deleted_record: str, delimiter: str, description: str, earliest_datestamp: datetime.date,
                 email: str, eprints: str, friends: str, granularity: str, identifier: str, institution: str,
                 metadata_policy: str, name: str, oai_identifier: str, protocol_version: str, purpose: str,
                 repository_identifier: str, repository_name: str, rights: str, rights_definition: str,
                 rights_manifest: str, sample_identifier: str, scheme: str, submission_policy: str,
                 text: str, title: str, toolkit: str, toolkit_icon: str, url: str, version: str, xoai_description: str,
                 grid_id: Union[str, None]):
        self.source_ref = source_ref
        self.source_url = source_url
        self.admin_email = admin_email
        self.author = author
        self.base_url = base_url
        self.comment = comment
        self.compression = compression
        self.content = content
        self.creator = creator
        self.data_policy = data_policy
        self.dc = dc
        self.deleted_record = deleted_record
        self.delimiter = delimiter
        self.description = description
        self.earliest_datestamp = earliest_datestamp
        self.email = email
        self.eprints = eprints
        self.friends = friends
        self.granularity = granularity
        self.identifier = identifier
        self.institution = institution
        self.metadata_policy = metadata_policy
        self.name = name
        self.oai_identifier = oai_identifier
        self.protocol_version = protocol_version
        self.purpose = purpose
        self.repository_identifier = repository_identifier
        self.repository_name = repository_name
        self.rights = rights
        self.rights_definition = rights_definition
        self.rights_manifest = rights_manifest
        self.sample_identifier = sample_identifier
        self.scheme = scheme
        self.submission_policy = submission_policy
        self.text = text
        self.title = title
        self.toolkit = toolkit
        self.toolkit_icon = toolkit_icon
        self.url = url
        self.version = version
        self.xoai_description = xoai_description
        self.grid_id = grid_id

    def get_id(self):
        # Source URL is used for the id, because we always have it. repository_identifier is not always provided.
        # TODO: strip out
        return unique_id(self.source_url)

    @staticmethod
    def from_dict(dict_):
        return Endpoint(dict_["source_ref"],
                        dict_["source_url"],
                        dict_["admin_email"],
                        dict_["author"],
                        dict_["base_url"],
                        dict_["comment"],
                        dict_["compression"],
                        dict_["content"],
                        dict_["creator"],
                        dict_["data_policy"],
                        dict_["dc"],
                        dict_["deleted_record"],
                        dict_["delimiter"],
                        dict_["description"],
                        dict_["earliest_datestamp"],
                        dict_["email"],
                        dict_["eprints"],
                        dict_["friends"],
                        dict_["granularity"],
                        dict_["identifier"],
                        dict_["institution"],
                        dict_["metadata_policy"],
                        dict_["name"],
                        dict_["oai_identifier"],
                        dict_["protocol_version"],
                        dict_["purpose"],
                        dict_["repository_identifier"],
                        dict_["repository_name"],
                        dict_["rights"],
                        dict_["rights_definition"],
                        dict_["rights_manifest"],
                        dict_["sample_identifier"],
                        dict_["scheme"],
                        dict_["submission_policy"],
                        dict_["text"],
                        dict_["title"],
                        dict_["toolkit"],
                        dict_["toolkit_icon"],
                        dict_["url"],
                        dict_["version"],
                        dict_["xoai_description"],
                        dict_["grid_id"])

    def to_dict(self):
        return {
            "source_ref": self.source_ref,
            "source_url": self.source_url,
            "admin_email": self.admin_email,
            "author": self.author,
            "base_url": self.base_url,
            "comment": self.comment,
            "compression": self.compression,
            "content": self.content,
            "creator": self.creator,
            "data_policy": self.data_policy,
            "dc": self.dc,
            "deleted_record": self.deleted_record,
            "delimiter": self.delimiter,
            "description": self.description,
            "earliest_datestamp": self.earliest_datestamp,
            "email": self.email,
            "eprints": self.eprints,
            "friends": self.friends,
            "granularity": self.granularity,
            "identifier": self.identifier,
            "institution": self.institution,
            "metadata_policy": self.metadata_policy,
            "name": self.name,
            "oai_identifier": self.oai_identifier,
            "protocol_version": self.protocol_version,
            "purpose": self.purpose,
            "repository_identifier": self.repository_identifier,
            "repository_name": self.repository_name,
            "rights": self.rights,
            "rights_definition": self.rights_definition,
            "rights_manifest": self.rights_manifest,
            "sample_identifier": self.sample_identifier,
            "scheme": self.scheme,
            "submission_policy": self.submission_policy,
            "text": self.text,
            "title": self.title,
            "toolkit": self.toolkit,
            "toolkit_icon": self.toolkit_icon,
            "url": self.url,
            "version": self.version,
            "xoai_description": self.xoai_description,
            "grid_id": self.grid_id
        }


class RecordHeader:
    def __init__(self, datestamp: datetime.date, deleted: bool, identifier: str):
        self.datestamp = datestamp
        self.deleted = deleted
        self.identifier = identifier

    @staticmethod
    def from_dict(dict_):
        return RecordHeader(dict_["datestamp"], dict_["deleted"], dict_["identifier"])

    def to_dict(self):
        return {
            "datestamp": self.datestamp,
            "deleted": self.deleted,
            "identifier": self.identifier
        }


class Record:
    def __init__(self, identity_ref: any, identity_id: str, identity_source_url: str,
                 header: RecordHeader, title: List[str], creator: List[str], description: List[str],
                 date: Union[datetime.date, None], type: List[str], identifier: List[str], format: List[str],
                 subject: List[str], relation: List[str], publisher: List[str], contributor: List[str],
                 language: List[str]):
        self.identity_ref = identity_ref
        self.identity_id = identity_id
        self.identity_source_url = identity_source_url
        self.header = header
        self.title = title
        self.creator = creator
        self.description = description
        self.date = date
        self.type = type
        self.identifier = identifier
        self.format = format
        self.subject = subject
        self.relation = relation
        self.publisher = publisher
        self.contributor = contributor
        self.language = language

    def get_id(self):
        return unique_id(self.header.identifier)

    @staticmethod
    def from_dict(dict_):
        return Record(dict_["identity_ref"],
                      dict_["identity_id"],
                      dict_["identity_source_url"],
                      dict_["header"],
                      dict_["title"],
                      dict_["creator"],
                      dict_["description"],
                      dict_["date"],
                      dict_["type"],
                      dict_["identifier"],
                      dict_["format"],
                      dict_["subject"],
                      dict_["relation"],
                      dict_["publisher"],
                      dict_["contributor"],
                      dict_["language"])

    def to_dict(self):
        data = {
            "identity_ref": self.identity_ref,
            "identity_id": self.identity_id,
            "identity_source_url": self.identity_source_url,
            "header": self.header.to_dict(),
            "title": self.title,
            "creator": self.creator,
            "description": self.description,
            "date": self.date,
            "type": self.type,
            "identifier": self.identifier,
            "format": self.format,
            "subject": self.subject,
            "relation": self.relation,
            "publisher": self.publisher,
            "contributor": self.contributor,
            "language": self.language,
        }

        return data

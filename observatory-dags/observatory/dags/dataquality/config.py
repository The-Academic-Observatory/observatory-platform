#!/usr/bin/python3

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


class JinjaParams:
    """ Jinja parameters. """

    PKG_NAME = 'observatory.dags.dataquality'
    TEMPLATE_PATHS = 'sql'


class MagCacheKey:
    """ Cache key names.  The date string format is %Y%m%d """

    RELEASES = 'releases'  # MAG release dates
    FOSL0 = 'fosl0_'  # concatenated with a date string. List of MAG FoS Level 0 fields.
    DOC_TYPE = 'doctype_'  # concatenated with a date string. Doctype from a release.
    FOS_LEVELS = 'foslevels_'  # concatenated with a date string. Field of study levels for a release.

    # concatenate with date string of release then '-' then fieldid then '-' then year
    FOSL0_FIELD_YEAR_SCORES = 'fosl0fys_'


class MagTableKey:
    """ BQ table and column names. """

    TID_PAPERS = 'Papers'
    TID_FOS = 'FieldsOfStudy'

    COL_PAP_COUNT = 'PaperCount'
    COL_CIT_COUNT = 'CitationCount'
    COL_NORM_NAME = 'NormalizedName'
    COL_FOS_ID = 'FieldOfStudyId'
    COL_DOI = 'Doi'
    COL_YEAR = 'Year'
    COL_FAMILY_ID = 'FamilyId'
    COL_DOC_TYPE = 'DocType'
    COL_LEVEL = 'Level'
    COL_PUBLISHER = 'Publisher'


class MagParams:
    BQ_SESSION_LIMIT = 10  # BQ limit is 100.

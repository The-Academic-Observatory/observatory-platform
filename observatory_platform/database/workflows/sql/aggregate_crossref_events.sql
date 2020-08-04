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

# Author: Richard Hosking

SELECT 
  (SUBSTR(obj_id, 17)) as doi,
  [
    STRUCT("twitter" as source, COUNTIF(source_id = 'twitter') as count),
    STRUCT("wikipedia" as source, COUNTIF(source_id = 'wikipedia') as count),
    STRUCT("newsfeed" as source, COUNTIF(source_id = 'newsfeed') as count),
    STRUCT("reddit-links" as source, COUNTIF(source_id = 'reddit-links') as count),
    STRUCT("reddit" as source, COUNTIF(source_id = 'reddit') as count),
    STRUCT("datacite" as source, COUNTIF(source_id = 'datacite') as count),
    STRUCT("wordpressdotcom" as source, COUNTIF(source_id = 'wordpressdotcom') as count),
    STRUCT("plaudit" as source, COUNTIF(source_id = 'plaudit') as count),
    STRUCT("stackexchange" as source, COUNTIF(source_id = 'stackexchange') as count),
    STRUCT("cambia-lens" as source, COUNTIF(source_id = 'cambia-lens') as count),
    STRUCT("hypothesis" as source, COUNTIF(source_id = 'hypothesis') as count),
    STRUCT("f1000" as source, COUNTIF(source_id = 'f1000') as count),
    STRUCT("web" as source, COUNTIF(source_id = 'web') as count),
    STRUCT("crossref" as source, COUNTIF(source_id = 'crossref') as count)
   ] as events
FROM `@crossref_events` 
GROUP BY
obj_id
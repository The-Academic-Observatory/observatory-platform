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
    dois.doi as doi,
    ARRAY_AGG(STRUCT(funder, fundref)) as funders
FROM `academic-observatory.doi.dois_2020_02_12` as dois, UNNEST(crossref.funder) as funder
LEFT JOIN `academic-observatory-telescope.fund_ref.fundref_latest` as fundref on SUBSTR(fundref.funder, 19) = funder.doi
GROUP BY dois.doi
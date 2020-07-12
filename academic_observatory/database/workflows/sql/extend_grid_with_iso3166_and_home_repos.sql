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
  grids.*,
  home_repo.urls as home_repo,
  STRUCT(regions) as iso3166
FROM `academic-observatory-telescope.grid.grid_2019_12_10` as grids
LEFT JOIN `academic-observatory-telescope.iso3166.iso3166_countries_and_regions` as regions ON (grids.addresses[OFFSET(0)].country_code = regions.alpha2)
LEFT JOIN `academic-observatory-telescope.coki_institution_repository.grid_home_repos` as home_repo ON grids.id = home_repo.grid_id 
WHERE ARRAY_LENGTH(grids.addresses) > 0
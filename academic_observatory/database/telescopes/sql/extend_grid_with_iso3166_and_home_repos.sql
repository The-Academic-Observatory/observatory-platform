SELECT 
  grids.*,
  home_repo.urls as home_repo,
  STRUCT(regions) as iso3166
FROM `academic-observatory-telescope.grid.grid_2019_12_10` as grids
LEFT JOIN `academic-observatory-telescope.iso3166.iso3166_countries_and_regions` as regions ON (grids.addresses[OFFSET(0)].country_code = regions.alpha2)
LEFT JOIN `academic-observatory-telescope.coki_institution_repository.grid_home_repos` as home_repo ON grids.id = home_repo.grid_id 
WHERE ARRAY_LENGTH(grids.addresses) > 0
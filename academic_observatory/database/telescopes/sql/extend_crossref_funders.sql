SELECT 
    dois.doi as doi,
    ARRAY_AGG(STRUCT(funder, fundref)) as funders
FROM `academic-observatory.doi.dois_2020_02_12` as dois, UNNEST(crossref.funder) as funder
LEFT JOIN `academic-observatory-telescope.fund_ref.fundref_latest` as fundref on SUBSTR(fundref.funder, 19) = funder.doi
GROUP BY dois.doi
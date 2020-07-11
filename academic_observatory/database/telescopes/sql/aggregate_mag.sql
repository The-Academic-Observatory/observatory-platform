SELECT 
  papers.*,
  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(JSON_EXTRACT(IndexedAbstract, '$.InvertedIndex'), "[0-9]+", ""), ":", ""), ",", " "), '"', ""), "{", ""), "}", ""), "[", ""), "]", "") as abstract,
  fields,
  authors,
  extended,
  resources,
  ARRAY((SELECT GridId FROM authors.authors WHERE GridId IS NOT NULL GROUP BY GridID)) as grids
FROM (SELECT doi, ARRAY_AGG(Paperid ORDER BY CitationCount DESC)[offset(0)] as PaperId
      FROM `academic-observatory-mag.mag_2019_10_03.Papers` as papers
      WHERE (papers.FamilyId is null OR papers.FamilyId = papers.PaperId) AND papers.doi IS NOT NULL
      GROUP BY doi) as dois

LEFT JOIN `academic-observatory-mag.mag_2019_10_03.Papers` as papers ON papers.PaperId = dois.PaperId

-- Abstract
LEFT JOIN `academic-observatory-mag.mag_2019_10_03.PaperAbstractsInvertedIndex` as abstracts ON abstracts.PaperId = papers.PaperId

-- Fields of Study
LEFT JOIN (SELECT 
              papers.PaperId, 
              -- Fields of Study
              STRUCT(
              ARRAY_AGG(IF(fields.Level = 0,STRUCT(fields.DisplayName,fields.FieldOfStudyId,fields.Rank,fields.MainType,paperFields.Score,extended.AttributeType as AttributeType,extended.AttributeValue as AttributeValue), null) IGNORE NULLS ORDER BY paperFields.Score DESC) as level_0,
              ARRAY_AGG(IF(fields.Level = 1, STRUCT(fields.DisplayName,fields.FieldOfStudyId,fields.Rank,fields.MainType,paperFields.Score,extended.AttributeType as AttributeType,extended.AttributeValue as AttributeValue), null) IGNORE NULLS ORDER BY paperFields.Score DESC) as level_1,
              ARRAY_AGG(IF(fields.Level = 2, STRUCT(fields.DisplayName,fields.FieldOfStudyId,fields.Rank,fields.MainType,paperFields.Score,extended.AttributeType as AttributeType,extended.AttributeValue as AttributeValue), null) IGNORE NULLS ORDER BY paperFields.Score DESC) as level_2,
              ARRAY_AGG(IF(fields.Level = 3, STRUCT(fields.DisplayName,fields.FieldOfStudyId,fields.Rank,fields.MainType,paperFields.Score,extended.AttributeType as AttributeType,extended.AttributeValue as AttributeValue), null) IGNORE NULLS ORDER BY paperFields.Score DESC) as level_3,
              ARRAY_AGG(IF(fields.Level = 4, STRUCT(fields.DisplayName,fields.FieldOfStudyId,fields.Rank,fields.MainType,paperFields.Score,extended.AttributeType as AttributeType,extended.AttributeValue as AttributeValue), null) IGNORE NULLS ORDER BY paperFields.Score DESC) as level_4,
              ARRAY_AGG(IF(fields.Level = 5, STRUCT(fields.DisplayName,fields.FieldOfStudyId,fields.Rank,fields.MainType,paperFields.Score,extended.AttributeType as AttributeType,extended.AttributeValue as AttributeValue), null) IGNORE NULLS ORDER BY paperFields.Score DESC) as level_5) as fields
            FROM `academic-observatory-mag.mag_2019_06_07.Papers`  as papers
            LEFT JOIN `academic-observatory-mag.mag_2019_10_03.PaperFieldsOfStudy` as paperFields on papers.PaperId = paperFields.PaperId
            LEFT JOIN `academic-observatory-mag.mag_2019_10_03.FieldsOfStudy` as fields on fields.FieldOfStudyId = paperFields.FieldOfStudyId
            LEFT JOIN `academic-observatory-mag.mag_2019_10_03.FieldOfStudyExtendedAttributes` as extended on extended.FieldOfStudyId = fields.FieldOfStudyId
            WHERE papers.Doi IS NOT NULL
            GROUP BY papers.PaperId) as fields ON fields.PaperId = papers.PaperId

-- Authors
LEFT JOIN (SELECT 
              papers.PaperId, 
              ARRAY_AGG(STRUCT(paperAuthorAffiliations.AuthorSequenceNumber, paperAuthorAffiliations.AuthorID, paperAuthorAffiliations.OriginalAuthor, paperAuthorAffiliations.AffiliationId, paperAuthorAffiliations.OriginalAffiliation, affiliation.GridId, affiliation.DisplayName) IGNORE NULLS ORDER BY paperAuthorAffiliations.AuthorSequenceNumber ASC) as authors
            FROM `academic-observatory-mag.mag_2019_10_03.Papers`  as papers
            LEFT JOIN `academic-observatory-mag.mag_2019_10_03.PaperAuthorAffiliations` as paperAuthorAffiliations on paperAuthorAffiliations.PaperId = papers.PaperId 
            LEFT JOIN `academic-observatory-mag.mag_2019_10_03.Affiliations` as affiliation on affiliation.AffiliationId = paperAuthorAffiliations.AffiliationId 
            GROUP BY papers.PaperId) as authors ON authors.PaperId = papers.PaperId

-- Extended Attributes
LEFT JOIN (SELECT
              PaperId,
              ARRAY_AGG(STRUCT( AttributeType, AttributeValue)) as attributes
            FROM `academic-observatory-mag.mag_2019_10_03.PaperExtendedAttributes`
            GROUP BY PaperId) as extended ON extended.PaperId = papers.PaperId

-- Resources
LEFT JOIN (SELECT
              PaperId,
              ARRAY_AGG(STRUCT( ResourceType , ResourceUrl )) as resources
            FROM `academic-observatory-mag.mag_2019_10_03.PaperResources` 
            GROUP BY PaperId) as resources ON resources.PaperId = papers.PaperId
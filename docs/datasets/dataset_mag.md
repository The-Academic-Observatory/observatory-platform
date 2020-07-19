# Datasets: Microsoft Academic Graph

"The Microsoft Academic Graph is a heterogeneous graph containing scientific publication 
records, citation relationships between those publications, as well as authors, institutions, 
journals, conferences, and fields of study.”
The dataset is provided as a set of related tables of data
. _- source: [MAG](https://www.microsoft.com/en-us/research/project/microsoft-academic-graph/
)_ 
and [data details](https://docs.microsoft.com/en-us/academic-services/graph/reference-data-schema
)

## Schema

---

**MAG Authors**

+ **AuthorId** [*Integer*]
+ **Rank** [*Integer*]
+ **NormalizedName** [*String*]
+ **DisplayName** [*String*]
+ **LastKnownAffiliationId** [*Integer*]
+ **PaperCount** [*Integer*]
+ **PaperFamilyCount** [*Integer*]
+ **CitationCount** [*Integer*]
+ **CreatedDate** [*Datetime*]

---

**MAG Affiliations**

+ **AffiliationId** [*Integer*]
+ **Rank** [*Integer*]
+ **NormalizedName** [*String*]
+ **DisplayName** [*String*]
+ **GridId** [*String*]
+ **OfficialPage** [*String*]
+ **WikiPage** [*String*]
+ **PaperCount** [*Integer*]
+ **CitationCount** [*Integer*]
+ **PaperFamilyCount** [*Integer*]
+ **Latitude** [*Float*]
+ **Longitude** [*Float*]
+ **CreatedDate** [*Datetime*]

---

**MAG Fields Of Study**

+ **FieldOfStudyId** [*Integer*]
+ **Rank** [*Integer*]
+ **NormalizedName** [*String*]
+ **DisplayName** [*String*]
+ **MainType** [*String*]
+ **Level** [*Integer*]
+ **PaperCount** [*Integer*]
+ **PaperFamilyCount** [*Integer*]
+ **CitationCount** [*Integer*]
+ **CreatedDate** [*String*]

---

**MAG Field Of Study Extended Attributes**

+ **FieldOfStudyId** [*Integer*]
+ **AttributeType** [*Integer*]
+ **AttributeValue** [*String*]

---

**MAG Related Field Of Study**

+ **FieldOfStudyId1** [*Integer*]
+ **Type1** [*String*]
+ **FieldOfStudyId2** [*Integer*]
+ **Type2** [*String*]
+ **Rank** [*Float*]

---

**MAG Field Of Study Children**

+ **FieldOfStudyId** [*Integer*]
+ **ChildFieldOfStudyId** [*Integer*]

---

**MAG Journals**

+ **JournalId** [*Integer*]
+ **Rank** [*Integer*]
+ **NormalizedName** [*String*]
+ **DisplayName** [*String*]
+ **Issn** [*String*]
+ **Publisher** [*String*]
+ **Webpage** [*String*]
+ **PaperCount** [*Integer*]
+ **PaperFamilyCount** [*Integer*]
+ **CitationCount** [*Integer*]
+ **CreatedDate** [*Datetime*]

---

**MAG Paper Abstracts Inverted Index**

+ **PaperId** [*Integer*]
+ **IndexedAbstract** [*String*]

---

**MAG Paper Author Affiliations**

+ **PaperId** [*Integer*]
+ **AuthorId** [*Integer*]
+ **AffiliationId** [*Integer*]
+ **AuthorSequenceNumber** [*Integer*]
+ **OriginalAuthor** [*String*]
+ **OriginalAffiliation** [*String*]

---

**MAG Paper Citation Contexts**

+ **PaperId** [*Integer*]
+ **PaperReferenceId** [*Integer*]
+ **CitationContext** [*String*]

---

**MAG Paper Extended Attributes**

+ **PaperId** [*Integer*]
+ **AttributeType** [*Integer*]
+ **AttributeValue** [*String*]

---

**MAG Paper Fields Of Study**

+ **PaperId** [*Integer*]
+ **FieldOfStudyId** [*Integer*]
+ **Score** [*Float*]

---

**MAG Paper Languages**

+ **PaperId** [*Integer*]
+ **LanguageCode** [*String*]

---

**MAG Paper Recommendations**

+ **PaperId** [*Integer*]
+ **RecommendedPaperId** [*Integer*]
+ **Score** [*Float*]

---

**MAG Paper References**

+ **PaperId** [*Integer*]
+ **PaperReferenceId** [*Integer*]

---

**MAG Paper Resources**

+ **PaperId** [*Integer*]
+ **ResourceType** [*Integer*]
+ **ResourceUrl** [*String*]
+ **SourceUrl** [*String*]
+ **RelationshipType** [*Integer*]

---

**MAG Paper Urls**

+ **PaperId** [*Integer*]
+ **SourceType** [*Integer*]
+ **SourceUrl** [*String*]
+ **LanguageCode** [*String*]

---

**MAG Papers** (for data extraction prior to 2020-05-21)

+ **PaperId** [*Integer*]
+ **Rank** [*Integer*]
+ **Doi** [*String*]
+ **DocType** [*String*]
+ **PaperTitle** [*String*]
+ **OriginalTitle** [*String*]
+ **BookTitle** [*String*]
+ **Year** [*Integer*]
+ **Date** [*Datetime*]
+ **Publisher** [*String*]
+ **JournalId** [*Integer*]
+ **ConferenceSeriesId** [*Integer*]
+ **ConferenceInstanceId** [*Integer*]
+ **Volume** [*String*]
+ **Issue** [*String*]
+ **FirstPage** [*String*]
+ **LastPage** [*String*]
+ **ReferenceCount** [*Integer*]
+ **CitationCount** [*Integer*]
+ **EstimatedCitation** [*Integer*]
+ **OriginalVenue** [*String*]
+ **FamilyId** [*Integer*]
+ **CreatedDate** [*Datetime*]

---

**MAG Papers** (for data extraction from 2020-06-05 onwards)

+ **PaperId** [*Integer*]
+ **Rank** [*Integer*]
+ **Doi** [*String*]
+ **DocType** [*String*]
+ **PaperTitle** [*String*]
+ **OriginalTitle** [*String*]
+ **BookTitle** [*String*]
+ **Year** [*Integer*]
+ **Date** [*Datetime*]
+ **OnlineDate** [*Datetime*]
+ **Publisher** [*String*]
+ **JournalId** [*Integer*]
+ **ConferenceSeriesId** [*Integer*]
+ **ConferenceInstanceId** [*Integer*]
+ **Volume** [*String*]
+ **Issue** [*String*]
+ **FirstPage** [*String*]
+ **LastPage** [*String*]
+ **ReferenceCount** [*Integer*]
+ **CitationCount** [*Integer*]
+ **EstimatedCitation** [*Integer*]
+ **OriginalVenue** [*String*]
+ **FamilyId** [*Integer*]
+ **CreatedDate** [*Datetime*]

---

**MAG Conference Instances**

+ **ConferenceInstanceId** [*Integer*]
+ **NormalizedName** [*String*]
+ **DisplayName** [*String*]
+ **ConferenceSeriesId** [*Integer*]
+ **Location** [*String*]
+ **OfficialUrl** [*String*]
+ **StartDate** [*Datetime*]
+ **EndDate** [*Datetime*]
+ **AbstractRegistrationDate** [*Datetime*]
+ **SubmissionDeadlineDate** [*Datetime*]
+ **NotificationDueDate** [*Datetime*]
+ **FinalVersionDueDate** [*Datetime*]
+ **PaperCount** [*Integer*]
+ **PaperFamilyCount** [*Integer*]
+ **CitationCount** [*Integer*]
+ **Latitude** [*Float*]
+ **Longitude** [*Float*]
+ **CreatedDate** [*Datetime*]

---

**MAG Conference Series**

+ **ConferenceSeriesId** [*Integer*]
+ **Rank** [*Integer*]
+ **NormalizedName** [*String*]
+ **DisplayName** [*String*]
+ **PaperCount** [*Integer*]
+ **PaperFamilyCount** [*Integer*]
+ **CitationCount** [*Integer*]
+ **CreatedDate** [*Datetime*]

---

**MAG Entity Related Entities**

+ **EntityId** [*Integer*]
+ **EntityType** [*String*]
+ **RelatedEntityId** [*Integer*]
+ **RelatedEntityType** [*String*]
+ **RelatedType** [*Integer*]
+ **Score** [*Float*]



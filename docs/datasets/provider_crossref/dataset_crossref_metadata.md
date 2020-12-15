# Crossref Metadata

Crossref Members send Crossref scholarly metadata on research which is collated and 
standardised into the Crossref metadata dataset. This dataset is made available through 
services and tools for manuscript tracking, searching, bibliographic management, 
library systems, author profiling, specialist subject databases, scholarly sharing networks
. _- source: [Crossref Metadata](https://www.crossref.org/services/metadata-retrieval/)_ 
and [schema details](https://github.com/Crossref/rest-api-doc/blob/master/api_format.md)

---

**Schema**

```json
[
  {
    "description": "Name of work's publisher.",
    "mode": "NULLABLE",
    "name": "publisher",
    "type": "STRING"
  },
  {
    "description": "Work titles, including translated titles.",
    "mode": "REPEATED",
    "name": "title",
    "type": "STRING"
  },
  {
    "description": "Work titles in the work's original publication language.",
    "mode": "REPEATED",
    "name": "original_title",
    "type": "STRING"
  },
  {
    "description": "Short or abbreviated work titles",
    "mode": "REPEATED",
    "name": "short_title",
    "type": "STRING"
  },
  {
    "description": "Abstract as a JSON string or a JATS XML snippet encoded into a JSON string.",
    "mode": "NULLABLE",
    "name": "abstract",
    "type": "STRING"
  },
  {
    "description": "Deprecated. Same as references-count.",
    "mode": "NULLABLE",
    "name": "reference_count",
    "type": "INTEGER"
  },
  {
    "description": "Count of outbound references deposited with Crossref",
    "mode": "NULLABLE",
    "name": "references_count",
    "type": "INTEGER"
  },
  {
    "description": "Count of inbound references deposited with Crossref.",
    "mode": "NULLABLE",
    "name": "is_referenced_by_count",
    "type": "INTEGER"
  },
  {
    "description": "Currently always Crossref.",
    "mode": "NULLABLE",
    "name": "source",
    "type": "STRING"
  },
  {
    "description": "DOI prefix identifier of the form http://id.crossref.org/prefix/DOI_PREFIX.",
    "mode": "NULLABLE",
    "name": "prefix",
    "type": "FLOAT"
  },
  {
    "description": "DOI of the work.",
    "mode": "NULLABLE",
    "name": "DOI",
    "type": "STRING"
  },
  {
    "description": "URL form of the work's DOI.",
    "mode": "NULLABLE",
    "name": "URL",
    "type": "STRING"
  },
  {
    "description": "Member identifier of the form http://id.crossref.org/member/MEMBER_ID",
    "mode": "NULLABLE",
    "name": "member",
    "type": "INTEGER"
  },
  {
    "description": "Enumeration, one of the type ids from https://api.crossref.org/v1/types.",
    "mode": "NULLABLE",
    "name": "type",
    "type": "STRING"
  },
  {
    "description": "Date on which the DOI was first registered.",
    "fields": [
      {
        "description": "Seconds since UNIX epoch.",
        "mode": "NULLABLE",
        "name": "timestamp",
        "type": "INTEGER"
      },
      {
        "description": "ISO 8601 date time.",
        "mode": "NULLABLE",
        "name": "date_time",
        "type": "TIMESTAMP"
      },
      {
        "description": "Contains an ordered array of year, month, day of month. Only year is required. Note that the field contains a nested array, e.g. [ [ 2006, 5, 19 ] ] to conform to citeproc JSON dates",
        "mode": "REPEATED",
        "name": "date_parts",
        "type": "INTEGER"
      }
    ],
    "mode": "NULLABLE",
    "name": "created",
    "type": "RECORD"
  },
  {
    "description": "Date on which the work metadata was most recently updated.",
    "fields": [
      {
        "description": "Seconds since UNIX epoch.",
        "mode": "NULLABLE",
        "name": "timestamp",
        "type": "INTEGER"
      },
      {
        "description": "ISO 8601 date time.",
        "mode": "NULLABLE",
        "name": "date_time",
        "type": "TIMESTAMP"
      },
      {
        "description": "Contains an ordered array of year, month, day of month. Only year is required. Note that the field contains a nested array, e.g. [ [ 2006, 5, 19 ] ] to conform to citeproc JSON dates",
        "mode": "REPEATED",
        "name": "date_parts",
        "type": "INTEGER"
      }
    ],
    "mode": "NULLABLE",
    "name": "deposited",
    "type": "RECORD"
  },
  {
    "description": "Date on which the work metadata was most recently indexed. Re-indexing does not imply a metadata change, see deposited for the most recent metadata change date.",
    "fields": [
      {
        "description": "Seconds since UNIX epoch.",
        "mode": "NULLABLE",
        "name": "timestamp",
        "type": "INTEGER"
      },
      {
        "description": "ISO 8601 date time.",
        "mode": "NULLABLE",
        "name": "date_time",
        "type": "TIMESTAMP"
      },
      {
        "description": "Contains an ordered array of year, month, day of month. Only year is required. Note that the field contains a nested array, e.g. [ [ 2006, 5, 19 ] ] to conform to citeproc JSON dates",
        "mode": "REPEATED",
        "name": "date_parts",
        "type": "INTEGER"
      }
    ],
    "mode": "NULLABLE",
    "name": "indexed",
    "type": "RECORD"
  },
  {
    "description": "Earliest of published-print and published-online",
    "fields": [
      {
        "description": "Contains an ordered array of year, month, day of month. Only year is required. Note that the field contains a nested array, e.g. [ [ 2006, 5, 19 ] ] to conform to citeproc JSON dates",
        "mode": "REPEATED",
        "name": "date_parts",
        "type": "INTEGER"
      }
    ],
    "mode": "NULLABLE",
    "name": "issued",
    "type": "RECORD"
  },
  {
    "description": "Date on which posted content was made available online.",
    "fields": [
      {
        "description": "Contains an ordered array of year, month, day of month. Only year is required. Note that the field contains a nested array, e.g. [ [ 2006, 5, 19 ] ] to conform to citeproc JSON dates",
        "mode": "REPEATED",
        "name": "date_parts",
        "type": "INTEGER"
      }
    ],
    "mode": "NULLABLE",
    "name": "posted",
    "type": "RECORD"
  },
  {
    "description": "Date on which a work was accepted, after being submitted, during a submission process.",
    "fields": [
      {
        "description": "Contains an ordered array of year, month, day of month. Only year is required. Note that the field contains a nested array, e.g. [ [ 2006, 5, 19 ] ] to conform to citeproc JSON dates",
        "mode": "REPEATED",
        "name": "date_parts",
        "type": "INTEGER"
      }
    ],
    "mode": "NULLABLE",
    "name": "accepted",
    "type": "RECORD"
  },
  {
    "description": "Work subtitles, including original language and translated.",
    "mode": "REPEATED",
    "name": "subtitle",
    "type": "STRING"
  },
  {
    "description": "Full titles of the containing work (usually a book or journal)",
    "mode": "REPEATED",
    "name": "container_title",
    "type": "STRING"
  },
  {
    "description": "Abbreviated titles of the containing work.",
    "mode": "REPEATED",
    "name": "short_container_title",
    "type": "STRING"
  },
  {
    "description": "Group title for posted content.",
    "mode": "NULLABLE",
    "name": "group_title",
    "type": "STRING"
  },
  {
    "description": "Issue number of an article's journal.",
    "mode": "NULLABLE",
    "name": "issue",
    "type": "STRING"
  },
  {
    "description": "Volume number of an article's journal.",
    "mode": "NULLABLE",
    "name": "volume",
    "type": "STRING"
  },
  {
    "description": "Pages numbers of an article within its journal.",
    "mode": "NULLABLE",
    "name": "page",
    "type": "STRING"
  },
  {
    "description": "",
    "mode": "NULLABLE",
    "name": "article_number",
    "type": "STRING"
  },
  {
    "description": "Date on which the work was published in print.",
    "fields": [
      {
        "description": "Contains an ordered array of year, month, day of month. Only year is required. Note that the field contains a nested array, e.g. [ [ 2006, 5, 19 ] ] to conform to citeproc JSON dates",
        "mode": "REPEATED",
        "name": "date_parts",
        "type": "INTEGER"
      }
    ],
    "mode": "NULLABLE",
    "name": "published_print",
    "type": "RECORD"
  },
  {
    "description": "Date on which the work was published online.",
    "fields": [
      {
        "description": "Contains an ordered array of year, month, day of month. Only year is required. Note that the field contains a nested array, e.g. [ [ 2006, 5, 19 ] ] to conform to citeproc JSON dates",
        "mode": "REPEATED",
        "name": "date_parts",
        "type": "INTEGER"
      }
    ],
    "mode": "NULLABLE",
    "name": "published_online",
    "type": "RECORD"
  },
  {
    "description": "Subject category names, a controlled vocabulary from Sci-Val. Available for most journal articles",
    "mode": "REPEATED",
    "name": "subject",
    "type": "STRING"
  },
  {
    "description": "",
    "mode": "REPEATED",
    "name": "ISSN",
    "type": "STRING"
  },
  {
    "description": "List of ISSNs with ISSN type information",
    "fields": [
      {
        "description": "ISSN type, can either be print ISSN or electronic ISSN.",
        "mode": "NULLABLE",
        "name": "type",
        "type": "STRING"
      },
      {
        "description": "ISSN value",
        "mode": "NULLABLE",
        "name": "value",
        "type": "STRING"
      }
    ],
    "mode": "REPEATED",
    "name": "issn_type",
    "type": "RECORD"
  },
  {
    "description": "",
    "mode": "REPEATED",
    "name": "ISBN",
    "type": "STRING"
  },
  {
    "description": "",
    "mode": "REPEATED",
    "name": "archive",
    "type": "STRING"
  },
  {
    "description": "",
    "fields": [
      {
        "description": "Either vor (version of record,) am (accepted manuscript,) tdm (text and data mining) or unspecified.",
        "mode": "NULLABLE",
        "name": "content_version",
        "type": "STRING"
      },
      {
        "description": "Number of days between the publication date of the work and the start date of this license.",
        "mode": "NULLABLE",
        "name": "delay_in_days",
        "type": "INTEGER"
      },
      {
        "description": "Date on which this license begins to take effect",
        "fields": [
          {
            "description": "Seconds since UNIX epoch.",
            "mode": "NULLABLE",
            "name": "timestamp",
            "type": "INTEGER"
          },
          {
            "description": "ISO 8601 date time.",
            "mode": "NULLABLE",
            "name": "date_time",
            "type": "STRING"
          },
          {
            "description": "Contains an ordered array of year, month, day of month. Only year is required. Note that the field contains a nested array, e.g. [ [ 2006, 5, 19 ] ] to conform to citeproc JSON dates",
            "mode": "REPEATED",
            "name": "date_parts",
            "type": "INTEGER"
          }
        ],
        "mode": "NULLABLE",
        "name": "start",
        "type": "RECORD"
      },
      {
        "description": "Link to a web page describing this license",
        "mode": "NULLABLE",
        "name": "URL",
        "type": "STRING"
      }
    ],
    "mode": "REPEATED",
    "name": "license",
    "type": "RECORD"
  },
  {
    "description": "",
    "fields": [
      {
        "description": "Funding body primary name",
        "mode": "NULLABLE",
        "name": "name",
        "type": "STRING"
      },
      {
        "description": "Optional Open Funder Registry DOI uniquely identifing the funding body (http://www.crossref.org/fundingdata/registry.html)",
        "mode": "NULLABLE",
        "name": "DOI",
        "type": "STRING"
      },
      {
        "description": "Award number(s) for awards given by the funding body.",
        "mode": "REPEATED",
        "name": "award",
        "type": "STRING"
      },
      {
        "description": "Either crossref or publisher",
        "mode": "NULLABLE",
        "name": "doi_asserted_by",
        "type": "STRING"
      }
    ],
    "mode": "REPEATED",
    "name": "funder",
    "type": "RECORD"
  },
  {
    "description": "",
    "fields": [
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "name",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "value",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "url",
        "type": "STRING"
      },
      {
        "description": "",
        "fields": [
          {
            "description": "",
            "mode": "NULLABLE",
            "name": "URL",
            "type": "STRING"
          }
        ],
        "mode": "NULLABLE",
        "name": "explanation",
        "type": "RECORD"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "label",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "order",
        "type": "INTEGER"
      },
      {
        "description": "",
        "fields": [
          {
            "description": "",
            "mode": "NULLABLE",
            "name": "name",
            "type": "STRING"
          },
          {
            "description": "",
            "mode": "NULLABLE",
            "name": "label",
            "type": "STRING"
          }
        ],
        "mode": "NULLABLE",
        "name": "group",
        "type": "RECORD"
      }
    ],
    "mode": "REPEATED",
    "name": "assertion",
    "type": "RECORD"
  },
  {
    "description": "",
    "fields": [
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "family",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "given",
        "type": "STRING"
      },
      {
        "description": "URL-form of an ORCID identifier",
        "mode": "NULLABLE",
        "name": "ORCID",
        "type": "STRING"
      },
      {
        "description": "If true, record owner asserts that the ORCID user completed ORCID OAuth authentication.",
        "mode": "NULLABLE",
        "name": "authenticated_orcid",
        "type": "BOOLEAN"
      },
      {
        "description": "",
        "fields": [
          {
            "description": "",
            "mode": "NULLABLE",
            "name": "name",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "affiliation",
        "type": "RECORD"
      },
      {
        "mode": "NULLABLE",
        "name": "name",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "suffix",
        "type": "STRING"
      }
    ],
    "mode": "REPEATED",
    "name": "author",
    "type": "RECORD"
  },
  {
    "description": "",
    "fields": [
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "family",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "given",
        "type": "STRING"
      },
      {
        "description": "URL-form of an ORCID identifier",
        "mode": "NULLABLE",
        "name": "ORCID",
        "type": "STRING"
      },
      {
        "description": "If true, record owner asserts that the ORCID user completed ORCID OAuth authentication.",
        "mode": "NULLABLE",
        "name": "authenticated_orcid",
        "type": "BOOLEAN"
      },
      {
        "description": "",
        "fields": [
          {
            "description": "",
            "mode": "NULLABLE",
            "name": "name",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "affiliation",
        "type": "RECORD"
      },
      {
        "mode": "NULLABLE",
        "name": "name",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "suffix",
        "type": "STRING"
      }
    ],
    "mode": "REPEATED",
    "name": "editor",
    "type": "RECORD"
  },
  {
    "description": "",
    "fields": [
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "family",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "given",
        "type": "STRING"
      },
      {
        "description": "URL-form of an ORCID identifier",
        "mode": "NULLABLE",
        "name": "ORCID",
        "type": "STRING"
      },
      {
        "description": "If true, record owner asserts that the ORCID user completed ORCID OAuth authentication.",
        "mode": "NULLABLE",
        "name": "authenticated_orcid",
        "type": "BOOLEAN"
      },
      {
        "description": "",
        "fields": [
          {
            "description": "",
            "mode": "NULLABLE",
            "name": "name",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "affiliation",
        "type": "RECORD"
      },
      {
        "mode": "NULLABLE",
        "name": "name",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "suffix",
        "type": "STRING"
      }
    ],
    "mode": "REPEATED",
    "name": "chair",
    "type": "RECORD"
  },
  {
    "description": "",
    "fields": [
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "family",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "given",
        "type": "STRING"
      },
      {
        "description": "URL-form of an ORCID identifier",
        "mode": "NULLABLE",
        "name": "ORCID",
        "type": "STRING"
      },
      {
        "description": "If true, record owner asserts that the ORCID user completed ORCID OAuth authentication.",
        "mode": "NULLABLE",
        "name": "authenticated_orcid",
        "type": "BOOLEAN"
      },
      {
        "description": "",
        "fields": [
          {
            "description": "",
            "mode": "NULLABLE",
            "name": "name",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "affiliation",
        "type": "RECORD"
      },
      {
        "mode": "NULLABLE",
        "name": "name",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "suffix",
        "type": "STRING"
      }
    ],
    "mode": "REPEATED",
    "name": "translator",
    "type": "RECORD"
  },
  {
    "description": "",
    "fields": [
      {
        "description": "Date on which the update was published.",
        "fields": [
          {
            "description": "Seconds since UNIX epoch.",
            "mode": "NULLABLE",
            "name": "timestamp",
            "type": "INTEGER"
          },
          {
            "description": "ISO 8601 date time.",
            "mode": "NULLABLE",
            "name": "date_time",
            "type": "STRING"
          },
          {
            "description": "Contains an ordered array of year, month, day of month. Only year is required. Note that the field contains a nested array, e.g. [ [ 2006, 5, 19 ] ] to conform to citeproc JSON dates",
            "mode": "REPEATED",
            "name": "date_parts",
            "type": "INTEGER"
          }
        ],
        "mode": "NULLABLE",
        "name": "updated",
        "type": "RECORD"
      },
      {
        "description": "DOI of the updated work.",
        "mode": "NULLABLE",
        "name": "DOI",
        "type": "STRING"
      },
      {
        "description": "The type of update, for example retraction or correction.",
        "mode": "NULLABLE",
        "name": "type",
        "type": "STRING"
      },
      {
        "description": "A display-friendly label for the update type.",
        "mode": "NULLABLE",
        "name": "label",
        "type": "STRING"
      }
    ],
    "mode": "REPEATED",
    "name": "update_to",
    "type": "RECORD"
  },
  {
    "description": "Link to an update policy covering Crossmark updates for this work.",
    "mode": "NULLABLE",
    "name": "update_policy",
    "type": "STRING"
  },
  {
    "description": "URLs to full-text locations.",
    "fields": [
      {
        "description": "Either text-mining, similarity-checking or unspecified.",
        "mode": "NULLABLE",
        "name": "intended_application",
        "type": "STRING"
      },
      {
        "description": "Either vor (version of record,) am (accepted manuscript) or unspecified.",
        "mode": "NULLABLE",
        "name": "content_version",
        "type": "STRING"
      },
      {
        "description": "Content type (or MIME type) of the full-text object.",
        "mode": "NULLABLE",
        "name": "content_type",
        "type": "STRING"
      },
      {
        "description": "Direct link to a full-text download location.",
        "mode": "NULLABLE",
        "name": "URL",
        "type": "STRING"
      }
    ],
    "mode": "REPEATED",
    "name": "link",
    "type": "RECORD"
  },
  {
    "description": "",
    "fields": [
      {
        "description": "Identifier of the clinical trial.",
        "mode": "NULLABLE",
        "name": "clinical_trial_number",
        "type": "STRING"
      },
      {
        "description": "DOI of the clinical trial regsitry that assigned the trial number.",
        "mode": "NULLABLE",
        "name": "registry",
        "type": "STRING"
      },
      {
        "description": "One of preResults, results or postResults",
        "mode": "NULLABLE",
        "name": "type",
        "type": "STRING"
      }
    ],
    "mode": "REPEATED",
    "name": "clinical_trial_number",
    "type": "RECORD"
  },
  {
    "description": "Other identifiers for the work provided by the depositing member",
    "mode": "REPEATED",
    "name": "alternative_id",
    "type": "STRING"
  },
  {
    "description": "List of references made by the work",
    "fields": [
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "key",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "DOI",
        "type": "STRING"
      },
      {
        "description": "One of crossref or publisher.",
        "mode": "NULLABLE",
        "name": "doi_asserted_by",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "issue",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "first_page",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "volume",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "edition",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "component",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "standard_designator",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "standards_body",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "author",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "year",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "unstructured",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "journal_title",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "article_title",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "series_title",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "volume_title",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "ISSN",
        "type": "STRING"
      },
      {
        "description": "One of pissn or eissn",
        "mode": "NULLABLE",
        "name": "issn_type",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "ISBN",
        "type": "STRING"
      },
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "isbn_type",
        "type": "STRING"
      }
    ],
    "mode": "REPEATED",
    "name": "reference",
    "type": "RECORD"
  },
  {
    "description": "Information on domains that support Crossmark for this work.",
    "fields": [
      {
        "description": "",
        "mode": "NULLABLE",
        "name": "crossmark_restriction",
        "type": "BOOLEAN"
      },
      {
        "description": "",
        "mode": "REPEATED",
        "name": "domain",
        "type": "STRING"
      }
    ],
    "mode": "NULLABLE",
    "name": "content_domain",
    "type": "RECORD"
  },
  {
    "description": "Relations to other works.",
    "fields": [
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "has_format",
        "type": "RECORD"
      },
      {
        "mode": "REPEATED",
        "name": "cites",
        "type": "STRING"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "has_preprint",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "has_part",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "has_review",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_supplemented_by",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_part_of",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_identical_to",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "references",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "has_manifestation",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_reply_to",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_based_on",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_review_of",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "has_reply",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_replaced_by",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "has_comment",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "documents",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_version_of",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "has_version",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "has_related_material",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_compiled_by",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "has_translation",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_translation_of",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_preprint_of",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_referenced_by",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_supplement_to",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_variant_form_of",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "replaces",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_manifestation_of",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_related_material",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_basis_for",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_comment_on",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "continues",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_continued_by",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "has_derivation",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_data_basis_for",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_documented_by",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_derived_from",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "has_manuscript",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_manuscript_of",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "based_on_data",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_original_form_of",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "is_varient_form_of",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "id_type",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "id",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "asserted_by",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "requires",
        "type": "RECORD"
      }
    ],
    "mode": "NULLABLE",
    "name": "relation",
    "type": "RECORD"
  },
  {
    "description": "Location of work's publisher",
    "mode": "NULLABLE",
    "name": "publisher_location",
    "type": "STRING"
  },
  {
    "description": "",
    "mode": "NULLABLE",
    "name": "score",
    "type": "STRING"
  },
  {
    "description": "description NA",
    "fields": [
      {
        "description": "Contains an ordered array of year, month, day of month. Only year is required. Note that the field contains a nested array, e.g. [ [ 2006, 5, 19 ] ] to conform to citeproc JSON dates",
        "mode": "REPEATED",
        "name": "date_parts",
        "type": "INTEGER"
      }
    ],
    "mode": "NULLABLE",
    "name": "approved",
    "type": "RECORD"
  },
  {
    "description": "description NA",
    "fields": [
      {
        "description": "Contains an ordered array of year, month, day of month. Only year is required. Note that the field contains a nested array, e.g. [ [ 2006, 5, 19 ] ] to conform to citeproc JSON dates",
        "mode": "REPEATED",
        "name": "date_parts",
        "type": "INTEGER"
      }
    ],
    "mode": "NULLABLE",
    "name": "content_created",
    "type": "RECORD"
  },
  {
    "description": "description NA",
    "fields": [
      {
        "description": "Contains an ordered array of year, month, day of month. Only year is required. Note that the field contains a nested array, e.g. [ [ 2006, 5, 19 ] ] to conform to citeproc JSON dates",
        "mode": "REPEATED",
        "name": "date_parts",
        "type": "INTEGER"
      }
    ],
    "mode": "NULLABLE",
    "name": "content_updated",
    "type": "RECORD"
  },
  {
    "description": "description NA",
    "mode": "REPEATED",
    "name": "degree",
    "type": "STRING"
  },
  {
    "fields": [
      {
        "mode": "NULLABLE",
        "name": "name",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "location",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "acronym",
        "type": "STRING"
      },
      {
        "mode": "REPEATED",
        "name": "sponsor",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "number",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "theme",
        "type": "STRING"
      },
      {
        "fields": [
          {
            "description": "Contains an ordered array of year, month, day of month. Only year is required. Note that the field contains a nested array, e.g. [ [ 2006, 5, 19 ] ] to conform to citeproc JSON dates",
            "mode": "REPEATED",
            "name": "date_parts",
            "type": "INTEGER"
          }
        ],
        "mode": "NULLABLE",
        "name": "start",
        "type": "RECORD"
      },
      {
        "fields": [
          {
            "description": "Contains an ordered array of year, month, day of month. Only year is required. Note that the field contains a nested array, e.g. [ [ 2006, 5, 19 ] ] to conform to citeproc JSON dates",
            "mode": "REPEATED",
            "name": "date_parts",
            "type": "INTEGER"
          }
        ],
        "mode": "NULLABLE",
        "name": "end",
        "type": "RECORD"
      }
    ],
    "mode": "NULLABLE",
    "name": "event",
    "type": "RECORD"
  },
  {
    "description": "",
    "fields": [
      {
        "mode": "NULLABLE",
        "name": "name",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "acronym",
        "type": "STRING"
      }
    ],
    "mode": "NULLABLE",
    "name": "standards_body",
    "type": "RECORD"
  }
]

```
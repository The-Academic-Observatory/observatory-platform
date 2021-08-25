# OAPEN Metadata
The OAPEN Metadata telescope collects data from the OAPEN Metadata feeds.
OAPEN enables libraries and aggregators to use the metadata of all available titles in the OAPEN Library.  
The metadata is available in different formats and this telescope harvests the data in the CSV format.  
See the [OAPEN Metadata webpage](https://www.oapen.org/resources/15635975-metadata) for more information. 

```eval_rst
+------------------------------+---------+
| Summary                      |         |
+==============================+=========+
| Average runtime              |  5min   |
+------------------------------+---------+
| Average download size        |  50MB   |
+------------------------------+---------+
| Harvest Type                 |  URL    |
+------------------------------+---------+
| Harvest Frequency            | Weekly  |
+------------------------------+---------+
| Runs on remote worker        | True    |
+------------------------------+---------+
| Catchup missed runs          | False   |
+------------------------------+---------+
| Table Write Disposition      | Append  |
+------------------------------+---------+
| Update Frequency             | Daily   |
+------------------------------+---------+
| Credentials Required         | No      |
+------------------------------+---------+
| Uses Telescope Template      | Stream  |
+------------------------------+---------+
```

## Schedule
The CSV file containing metadata is updated daily and this telescope is scheduled to harvest the metadata weekly. 

## Results
There are two tables containing data related to this telescope:
  * The main table which contains all the up-to-date metadata. 
  * A partitioned table, where each partition contains the metadata of one run.

## Tasks
### download
This is where the metadata is downloaded. The CSV file containing metadata is downloaded using the CSV url that is 
available on the OAPEN Metadata webpage mentioned above.

### transform
The downloaded CSV is transformed into a gzipped jsonl format (this format is accepted by BigQuery). 
The flattened CSV file only is restructured into nested dictionaries and/or lists where appropriate.  
An additional field 'classification_code' is added as well. 
This is based on data in the field 'dc.subject.classification', but reformatted so that the codes are easy to use with other datasets.  
Below is an example of one row before and after transformation:

Before transformation
```yaml
{'id': 'af17b762-8a0c-4719-aa9c-7f5083a0ba52',
'collection': '20.500.12657/6',
'BITSTREAM Download URL': 'https://library.oapen.org/bitstream/20.500.12657/29427/1/academia-LLS.2018.10_160x240.pdf',
'BITSTREAM ISBN': '9789401452113',
'BITSTREAM License': 'http://creativecommons.org/licenses/by-nd/3.0/',
'BITSTREAM Webshop URL': '',
'dc.contributor.advisor': '',
'dc.contributor.author': 'Missinne  , Lut||Grave , Jaap',
'dc.contributor.editor': '',
'dc.contributor.other': '',
'dc.date.accessioned': '2018-09-12 23:55||2019-04-18 16:22:31||2020-04-01T12:27:51Z',
'dc.date.available': '2020-04-01T12:27:51Z',
'dc.date.issued': '2018',
'dc.description': '',
'dc.description.abstract': 'The abstract',
'dc.description.provenance': "Created by dorien on 2018-09-12 14:55:07||Edited by lotte on 2019-04-18 16:22:31||Made available in DSpace on 2020-04-01T12:27:51Z (GMT). No. of bitstreams: 1\nacademia-LLS.2018.10_160x240.pdf: 13825548 bytes,
checksum: a60085ebcce61de9bc7d22efc2924dce (MD5)\n  Previous issue date: 2018||Item created via OAI harvest from source: https://dataprovider.oapen.org/cgi/oapen on 2020-04-01T12:27:51Z (GMT).  Item's OAI Record identifier: oai:oapen:1000509",
'dc.description.version': '',
'dc.identifier': '1000509',
'dc.identifier.isbn': '',
'dc.identifier.issn': '',
'dc.identifier.uri': 'http://library.oapen.org/handle/20.500.12657/29427',
'dc.identifier.urlwebshop': '',
'dc.language': 'Dutch; Flemish[dut]||German[ger]',
'dc.relation.isnodouble': '',
'dc.relation.ispartofseries': 'Lage Landen Studies',
'dc.relation.isreplacedbydouble': '',
'dc.rights.uri': '',
'dc.source': '',
'dc.subject.classification': 'bic Book Industry Communication::C Language::CF linguistics::CFP Translation & interpretation||bic Book Industry Communication::D Literature & literary studies||bic Book Industry Communication::D Literature & literary studies::DS Literature: history & criticism::DSK Literary studies: fiction, novelists & prose writers||bic Book Industry Communication::F Fiction & related items',
'dc.subject.other': 'Dutch fiction||fiction in translation||translation||literary transfer||cultural transfer',
'dc.title': 'Tussen twee stoelen, tussen twee vuren',
'dc.title.alternative': 'Nederlandse literatuur op weg naar de buitenlandse lezer',
'dc.type': 'book',
'dcterms.abstract': '',
'eperson.firstname': '',
'eperson.language': '',
'eperson.lastname': '',
'eperson.phone': '',
'grantor.acronym': '',
'grantor.doi': '',
'grantor.jurisdiction': '',
'grantor.name': '',
'grantor.number': '',
'oapen.abstract.otherlanguage': 'Abstract other language',
'oapen.autodoi': '',
'oapen.chapternumber': '',
'oapen.collection': '',
'oapen.description.otherlanguage': '',
'oapen.embargo': '',
'oapen.grant.acronym': '',
'oapen.grant.number': '',
'oapen.grant.program': '',
'oapen.grant.project': '',
'oapen.identifier': '',
'oapen.identifier.doi': '',
'oapen.identifier.isbn': '',
'oapen.imprint': '',
'oapen.notes': '',
'oapen.pages': '222',
'oapen.place.publication': 'Gent',
'oapen.redirect': '',
'oapen.relation.funds': '',
'oapen.relation.hasChapter': '',
'oapen.relation.hasChapter_dc.title': '',
'oapen.relation.isFundedBy': '',
'oapen.relation.isFundedBy_grantor.name': 'Internationale Vereniging voor Neerlandistiek',
'oapen.relation.isPartOfBook': '',
'oapen.relation.isPartOfBook_dc.title': '',
'oapen.relation.isPublishedBy': '',
'oapen.relation.isPublishedBy_publisher.name': 'Academia Press',
'oapen.relation.isPublisherOf': '',
'oapen.relation.isbn': '',
'oapen.remark.private': '',
'oapen.remark.public': '21-7-2020 - No DOI registered in CrossRef for ISBN 9789401452113',
'oapen.series.number': '10',
'publisher.country': '',
'publisher.name': '',
'publisher.peerreviewpolicy': '',
'publisher.website': ''}
```

After transformation
```yaml
{
   "id":"af17b762-8a0c-4719-aa9c-7f5083a0ba52",
   "collection":[
      "20.500.12657/6"
   ],
   "BITSTREAM_Download_URL":[
      "https://library.oapen.org/bitstream/20.500.12657/29427/1/academia-LLS.2018.10_160x240.pdf"
   ],
   "BITSTREAM_ISBN":[
      "9789401452113"
   ],
   "BITSTREAM_License":[
      "http://creativecommons.org/licenses/by-nd/3.0/"
   ],
   "dc":{
      "contributor":{
         "author":[
            "Missinne  , Lut",
            "Grave , Jaap"
         ]
      },
      "date":{
         "accessioned":[
            "2018-09-12 23:55",
            "2019-04-18 16:22:31",
            "2020-04-01T12:27:51Z"
         ],
         "available":"2020-04-01T12:27:51Z",
         "issued":[
            "2018-01-01"
         ]
      },
      "description":{
         "abstract":[
            "The abstract."
         ],
         "provenance":[
            "Created by dorien on 2018-09-12 14:55:07",
            "Edited by lotte on 2019-04-18 16:22:31",
            "Made available in DSpace on 2020-04-01T12:27:51Z (GMT). No. of bitstreams: 1\nacademia-LLS.2018.10_160x240.pdf: 13825548 bytes, checksum: a60085ebcce61de9bc7d22efc2924dce (MD5)\n  Previous issue date: 2018",
            "Item created via OAI harvest from source: https://dataprovider.oapen.org/cgi/oapen on 2020-04-01T12:27:51Z (GMT).  Item's OAI Record identifier: oai:oapen:1000509"
         ]
      },
      "identifier":{
         "value":[
            "1000509"
         ],
         "uri":"http://library.oapen.org/handle/20.500.12657/29427"
      },
      "language":[
         "Dutch; Flemish[dut]",
         "German[ger]"
      ],
      "relation":{
         "ispartofseries":[
            "Lage Landen Studies"
         ]
      },
      "subject":{
         "classification":[
            "bic Book Industry Communication::C Language::CF linguistics::CFP Translation & interpretation",
            "bic Book Industry Communication::D Literature & literary studies",
            "bic Book Industry Communication::D Literature & literary studies::DS Literature: history & criticism::DSK Literary studies: fiction, novelists & prose writers",
            "bic Book Industry Communication::F Fiction & related items"
         ],
         "classification_code":[
            "CFP",
            "D",
            "DSK",
            "F"
         ],
         "other":[
            "Dutch fiction",
            "fiction in translation",
            "translation",
            "literary transfer",
            "cultural transfer"
         ]
      },
      "title":{
         "value":[
            "Tussen twee stoelen, tussen twee vuren"
         ],
         "alternative":"Nederlandse literatuur op weg naar de buitenlandse lezer"
      },
      "type":[
         "book"
      ]
   },
   "oapen":{
      "abstract":{
         "otherlanguage":"Abstract other language."
      },
      "pages":"222",
      "place":{
         "publication":"Gent"
      },
      "relation":{
         "isFundedBy_grantor":{
            "name":[
               "Internationale Vereniging voor Neerlandistiek"
            ]
         },
         "isPublishedBy_publisher":{
            "name":"Academia Press"
         }
      },
      "remark":{
         "public":[
            "21-7-2020 - No DOI registered in CrossRef for ISBN 9789401452113"
         ]
      },
      "series":{
         "number":"10"
      }
   }
}
```

## Latest schema
``` eval_rst
.. csv-table::
   :file: ../schemas/oapen_metadata_latest.csv
   :width: 100%
   :header-rows: 1
```
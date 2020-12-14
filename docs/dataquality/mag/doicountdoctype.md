# DoiCountDocTypeModule

This is a MAG Analyser module looks at the ```Papers``` table from MAG, and reports:
 * the type of document (its ```DocType```),
 * the total number of documents for each type,
 * the number of documents with a ```null``` doi entry for each type,
 * the proportion of documents with ```null``` doi entry for each type.

If no document type is specified, it will be lumped into a ```null``` category.

It produces ```MagDoiCountsDocType``` documents in elastic search with the index
```dataquality-mag-doi-counts```.
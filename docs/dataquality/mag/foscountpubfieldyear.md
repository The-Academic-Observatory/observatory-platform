# FosCountsPubFieldYearModule

This is a MAG Analyser module that computes paper, citation, and reference counts per publisher, per field, per year. It currently grabs the top 1000 publishers in each category.

If the year is not specified, it will be lumped into a ```null``` category.  If the publisher is not specified, it will be lumped into a ```null``` category.

It produces ```MagFosCountPubFieldYear``` documents in elastic search with the index
```dataquality-mag-fos-counts-pub-field-year```.
# PaperMetricsModule

This is a MAG Analyser module that computes the:
 * total number of papers,
 * null counts for:
   * year,
   * doi,
   * doctype,
   * family id.

It also computes the proportional counts with respect to their categories.

It produces ```MagPapersMetrics``` documents in elastic search with the index
```dataquality-mag-papers-metrics```.
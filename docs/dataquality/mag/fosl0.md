# FieldsOfStudyLevel0Module

This is a MAG Analyser module that computes some metrics for level 0 fields of study labels.  These include:
 * paper counts,
 * citation counts,
 * count differences between releases,
 * count differences as a proportional change between releases,
 * whether the list of ids and names have changed.

 It produces ```MagFosL0Counts``` and ``` MagFosL0Metrics``` documents in elastic search with the indices
```dataquality-mag-fos-l0-counts``` and ```dataquality-mag-fos-l0-metrics```.
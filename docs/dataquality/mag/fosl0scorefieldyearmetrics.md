# FosL0ScoreFieldYearMetricsModule

This is a MAG Analyser module looks at the histogram of saliency scores for each level 0 field of study label, per year, between two consecutive releases.  It calculates the Jensen Shannon distance between the two histograms.  The aim is to monitor any histogram shape changes between releases.

It produces ```MagFosL0ScoreFieldYearMetricY``` and ```MagFosL0ScoreFieldYearMetricR``` documents in elastic search with the indices ```dataquality-mag-fos-l0-score-field-year-metric-year``` and 
```dataquality-mag-fos-l0-score-field-year-metric-release``` respectively.
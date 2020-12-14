# FosL0ScoreFieldYearModule

This is a MAG Analyser module taht computes the histogram of saliency scores for each level 0 field of study, per year.

It produces ```MagFosL0ScoreFieldYear``` documents in elastic search with the index
```dataquality-mag-fos-l0-score-field-year```.

The ```score_start``` and ```score_end``` attributes define the half open interval of each slice (not inclusive of the endpoint), except for the last interval, which does include its endpoint.
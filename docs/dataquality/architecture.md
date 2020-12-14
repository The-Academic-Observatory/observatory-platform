# Python architecture


## Analysers

An analyser is responsible for computing a profile for a given a data set.  Each analyser should only compute profile metrics for a single data set.

Analysers must implement the ```DataQualityAnalyser``` interface.

Analyser classes have the opportunity to specify their own constructors.  This is responsible for loading the relevant modules.

There is currently no mechanism for resolving dependencies, so care needs to be taken when specifying the order in which modules are loaded.

You can start execution of the analyser by invoking the ```run``` method.  This allows for scripts to control when to execute the analysers.

Analysers also have an ```erase``` method for quickly deleting documents in the database.  Since elastic search is currently used, specifying the optional keyword argument ```index=True``` (default is ```False```) will also delete the document index (not to be confused with the Kibana index).


## Modules

Most profile metrics are separated into individual modules.  Each individual module is responsible for:

 * fetching the relevant data from the data lake,
 * computing the metrics,
 * constructing the database records (elastic search documents),
 * saving the records (indexing the elastic search documents).

Currently, a specific module interface is defined for the Microsoft Academic Graph Analyser module (``` MagAnalyserModule ```).  We will revisit this at some point to see if it is better to have a more generic module definition.

This interface has ```run``` and ```erase``` methods to implement the help implement the functionality of the Analyser interface.  It also has a ```name``` method for fetching the name of the module.
# Introduction

This tutorial assumes you have a basic understanding of how Airflow works, and the process of creating Airflow DAGs.  [Airflow provide a tutorial on the introductory concepts.](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html)  Many workflows in the Academic Observatory follow similar design patterns like extract, transform, load (ETL). Telescopes aim to reduce some of the boiler plate code required in order to write some of the workflows used in the Academic Observatory platform.

## Telescopes

Each workflow in the Academic Observatory consists of set of tasks.  The tasks are organised as nodes on a directed acyclic graph (DAG) which describe the order of execution, and dependencies. Telescope tasks are Python functions with a specific signature, that are turned into Airflow PythonOperator objects.  The exception is the sensor tasks, which are Airflow Sensor objects, e.g., ExternalTaskSensor.

The telescope class groups tasks into sensor tasks, setup tasks, and generic tasks. The Telescope class provides methods to add tasks to each group.  Once all tasks have been added, there is a method to create the DAG object for Airflow to use.

<p align="center">
<img title="Telescope flow" alt="Telescope flow" src="../graphics/telescope_flow.png">
</p>

The entry point of the Telescope is the first sensor task.  Sensor tasks are Airflow Sensor objects that monitor for certain states and can block execution until it is in a particular state.  The sensors are chained together linearly in the order they are inserted.

The last sensor task connects to the first setup tasks to perform any required setup tasks.  Setup tasks are chained linearly in the order they inserted.

The last setup task connects to the first generic task. The generic tasks are chained linearly in the order they are inserted.  The endpoint of the telescope is the last task in the generic task list.

If any of those task groups are empty, execution skips to the next non empty group list.

## A typical development pipeline

A typical telescope pipeline will:
``` eval_rst
#. Create a DAG file for that telescope in observatory-dags/observatory/dags/dags that calls code to construct the telescope.
#. Create a telescope file for the telescope definition in observatory-dags/observatory/telescopes containing code for the telesecope itself.
#. Create tests in tests/observatory/dags/telescopes for the telescope.
#. Create documentation for the telescope in docs/telescopes.
```

## Creating a DAG file

For Airflow to pickup new DAGs, we currently require you to create a new Python file in `observatory-dags/observatory/dags/dags` with content similar to:

```
# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

from observatory.dags.telescopes.my_telescope import MyTelescope

telescope = MyTelescope()
globals()[telescope.dag_id] = telescope.make_dag()
```

## Creating a telescope file

Put your new telescope class in the directory `observatory-dags/observatory/telescopes`

## Release class

Datasets often have release information associated with each release, e.g., a date or release version.  Multiple releases can be ingested for each dataset on a regular schedule by the Acacdemic Observatory.  Release information for the dataset is stored in a release class object.  The release object is constructed and passed to each task in the telescope as a parameter. It is done this way because Airflow assumes each task is assumed to be sandboxed, so any inter-task communication requires RPC or local disk storage for communication.

The release class contains:
``` eval_rst
#. The DAG ID.
#. The release ID.
#. Regex patterns used to find files in the download, extract, and transform folders.
#. Lists of download, extract, and transform files.
#. A method for cleaning up the download, extract, and transform files.
```

The interface is defined in `AbstractRelease`. There is also a `Release` class you can use which implements all of those functions if you want to just let the API generate all those folders and file lists based on the `dag_id` and `release_id`.  See the API documentation for the `Release` class for more information.  Your telescope file should contain a **release** class if it's required for your workflow.

## Telescope class

The interface specification is defined in `AbstractTelescope`.  There is a `Telescope` class you can use with an implementation of that interface. Your telescope file should contain a telescope class.  See the API documentation for more details. The telesecope class is responsible for adding tasks to the DAG, constructing a DAG for Airflow to use, and onstructing release objects to pass to each task during execution.

## Example telescope file

```
import logging
import pendulum

from typing import List
from observatory.platform.telescopes.telescope import AbstractRelease, Telescope


class MyRelease(AbstractRelease):
    def __init__(self, dag_id, release_date):
        self.dag_id = dag_id
        self.release_date = release_date

    def download_bucket(self):  # Required
        return "download_bucket_name"

    def transform_bucket(self):  # Required
        return "transform_bucket_name"

    def download_folder(self):  # Required
        return "download_folder_name"

    def extract_folder(self):  # Required
        return "extract_folder_name"

    def transform_folder(self):  # Required
        return "transform_folder_name"

    def download_files(self):  # Required
        return ["list", "of", "download", "files"]

    def extract_files(self):  # Required
        return ["list", "of", "extract", "files"]

    def transform_files(self):  # Required
        return ["list", "of", "transform", "files"]

    def cleanup(self):  # Required
        # Cleanup code
        pass


class MyTelescope(Telescope):
    # dag_id, start_date, schedule_interval, catchup correspond to the DAG parameters in Airflow with the same name.
    def __init__(self, *, dag_id: str, start_date: pendulum.Pendulum = pendulum.Pendulum(2021, 1, 1), schedule_interval:str = "@weekly", catchup: bool = False):
        # Initialise base class
        super().__init__(
            dag_id=self.dag_id,
            start_date=self.start_date,
            schedule_interval=self.schedule_interval,
            catchup=self.catchup,
        )

        # Any other initialisation

        # Add sensor tasks
        # self.add_sensor(some_airflow_sensor)

        # Add setup tasks
        # self.add_setup_task(self.some_setup_task)

        # Add generic tasks
        self.add_task(self.task1)
        self.add_task(self.task2)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs):  # Required
        releases = list()
        release = MyRelease(dag_id=self.dag_id, release_date=self.start_date)
        releases.append(release)
        return releases

    def task1(self, releases: List[MyRelease], **kwargs):
        logging.warn("Task 1 executing")

    def task2(self, releases: List[MyRelease], **kwargs):
        logging.warn("Task 2 executing")

    def cleanup(self, releases: List[MyRelease], **kwargs):
        logging.warn("Cleanup task executing")
```

This telescope adds `task1, task2, cleanup` tasks that just print some statements. The function signature is always the same for Telescope tasks.   If you need to pass additional arguments, you should do it through keyword arguments, and process `**kwargs` within the task function.

If you start the observatory platform with these changes, you will see a new DAG in Airflow called **my_dag_id** with the DAG structure `task1 -> task2 -> cleanup`.  When you run the DAG, you should see the logging messages in the log of each task.

## BigQuery schemas

BigQuery database schema json files are put in `observatory-dags/dags/database/schema`.  They follow the scheme: `<table_name>_YYYY-MM-DD.json`.  If you wish to provide an additional custom version as well as the date, then the files should follow the scheme: `<table_name>_customversion_YYYY-MM-DD.json`.

The BigQuery table loading utility functions in the Academic Observatory platform API will try to find the correct schema to use for loading table data, based on release date information.

## Specialised telescopes

Currently there are two types of specialised telescope patterns implemented by the Academic Observatory.  These are:
```eval_rst
#. the `StreamTelescope`, where the datasets have an initial full snapshot, followed by differential releases, and
#. the `SnapshotTelescope`, where each release is a full snapshot of the data.
```

The stream and snapshot telescopes derive the `Telescope` class.

These telescopes provide extra methods for loading data into BigQuery.  Currently only Google Cloud Storage and BigQuery are supported.

If you wish to design your own telescope templates, you can choose to derive the `Telescope` class if it suits your needs, or implement the `AbstractTelescope` interface yourself.

## Generating a telescope template

You can use the observatory cli tool to generate a telescope template for a new `Telescope`, `StreamTelescope` or `SnapshotTelescope` telescope and release class. Use the command:
```
observatory generate telescope <type> <name>
```
where the type can be `Telescope`, `StreamTelescope`, `SnapshotTelescope` and name is the class name of your new telescope.  It will generate a new dag `.py` file in `observatory-dags/observatory/dags/dags` with the lower case class name.  Similarly a telescope template will be created in `observatory-dags/observatory/dags/telescopes` with the same lower case class name.

For example:
```
observatory generate telescope SnapshotTelescope MyNewTelescope
```
creates the files `observatory-dags/observatory/dags/dags/mynewtelescope.py` and `observatory-dags/observatory/dags/telescopes/mynewtelescope.py`

## Documentation

The Academic Observatory builds documentation using [Sphinx](https://www.sphinx-doc.org).  Documentation is contained in the `docs` directory. Currently index pages are written in [RST format (Restructured Text)](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html), and content pages are written with [Markdown](https://www.sphinx-doc.org/en/master/usage/markdown.html) for simplicity.

You can generate documentation by using the command:
```
cd docs
make html
```
This will output html documentation in the `docs/_build/html` directory.

### Including schemas in documentation

The documentation build system automatically converts all the schema files from `observatory-dags/observatory/dags/database/schemas` into CSV files.  This is temporarily stored in the `docs/schemas` folder. The csv files have the same filename as the original schema files, except for the suffix, which is changed to csv.  The schemas folder is cleaned up as part of the build process so you will not be able to see the directory unless you disable the cleanup code in the `Makefile`.

To include a schema in your documentation markdown file, we need to embed some RST that loads a table from a csv file. Since we use the recommonmark package, this can be done with an `eval_rst` codeblock that contains RST:

    ``` eval_rst
    .. csv-table::
    :file: /path/to/schema.csv
    :width: 100%
    :header-rows: 1
    ```

To figure out the file path, it is recommended you construct a relative path to the `docs/schemas` directory from the directory of your markdown file. For example, if your documentation file resides in
```
docs/datasets/mydataset
```
then you should set
```
:file: ../../schemas/myschemafile.csv
```
The `..` follows the parent directory, and we need to do this twice to reach `docs` from `docs/datasets/mydataset`.

## Style

We try to conform to the Python PEP-8 standard, and the default format style of the `Black` formatter.  This is done with the [autopep8 package](https://pypi.org/project/autopep8), and the [black formatter](https://pypi.org/project/black/).

We recommend you use those format tools as part of your coding workflow.

### Type hinting

You should provide type hints for all of the function arguments you use, and for return types.  Because Python is a weakly typed language, it can be confusing to those unacquainted with the codebase what type of objects are being manipulated in a particular function.  Type hints help reduce this ambiguity.

### Docstring

Please provide docstring comments for all your classes, methods, and functions.  This includes descriptions of arguments, and returned objects.  These comments will be automatically compiled into the Academic Observatory API reference documentation section.
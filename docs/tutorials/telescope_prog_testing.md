# Testing

The Academic Observatory provides a framework that aids in unit and functional testing.  The tests are found in the `tests` folder. Telescope tests are found in `tests/observatory/dags/telescopes`.

Academic Observatory uses the `unittest` Python framework as a base, and provides additional methods to run tasks and test DAG structure.  It also uses the Python `coverage` package to analyse test coverage.

Test methods can be found in the module `observatory.platform.utils.test_utils`.

To take advantage of the framework, instead of inheriting `unittest.TestCase` in test classes, each test class inherits `ObservatoryTestCase`.

## Testing DAGs

### DAG structure

The telescope's DAG can be tested through the `assert_dag_structure` method of `ObservatoryTestCase`.  The DAG object is compared against a dictionary, where the key is the source node, and value is a list of sink nodes.  This expresses the relationship that the source node task is a dependency of all of the sink node tasks.

Example:
```
from observatory.platform.utils.test_utils import ObservatoryTestCase
from observatory.platform.telescopes.telescope import Telescope

class MyTelescope(Telescope):
    def __init__(self):
      self.add_task(task1)
      self.add_task(task2)

    def make_release(self, **kwargs):
      return list()

    def task1(self, releases, **kwargs):
      pass

    def task2(self, releases, **kwargs):
      pass

class MyTestClass(ObservatoryTestCase):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

  def test_dag_structure(self):
    expected = {
      "task1": ["task2"],
      "task2": []
    }
    telescope = MyTelescope()
    dag = telescope.make_dag()
    self.assert_dag_structure(dag, expected)
```

## Functional testing

To functionally test without using Airflow itself, Academic Observatory provides the `ObservatoryEnvironment` class to simulate some of the functionality of Airflow.

### Dag loading

To test if a DAG loads from a DagBag, we can create an `ObservatoryEnvironment` object, and run `assert_dag_load` in the environment. Example:
```
# This is run within an ObservatoryTestCase class
env = ObservatoryEnvironment()
with env.create():
  dag_file = os.path.join(module_file_path("observatory.dags.dags"), "my_telescope")
  self.assert_dag_load("dag_id", dag_file)
```
The `module_file_path` method from `observatory.platform.utils.test_utils` finds the file path to the observatory module on the local filesystem.

### Running tasks

If you want to run a telescope task, you can call the `run_task` method from the `ObservatoryEnvironment` class.  Example:
```
class MyTelescope(Telescope):
    def __init__(self):
      self.add_task(task1)
      self.add_task(task2)

    def make_release(self, **kwargs):
      return list()

    def task1(self, releases, **kwargs):
      pass

class MyTest(ObservatoryTestCase):
  def test_something(self):
      telescope = MyTelescope()
      dag = telescope.make_dag()
      timestamp = pendulum.now("UTC")
      env = ObservatoryEnvironment()
      with env.create():  # Creates a session
        env.run_task(telescope.task1.__name__, dag, timestamp)
```

Note that the run dependencies imposed on each task by the DAG structure are preserved in the test environment.  If you want to run a specific task, you need to make sure all the previous tasks in the DAG are run first within the same `ObservatoryEnvironment` session.

### Temporary GCP datasets

Unit testing frameworks often run tests in parallel, so there is no guarantee of execution order.  If you are running code that modifies datasets or tables in the Google Cloud, then we recommend that you  create temporary datasets for each task to stop testing bugs caused by race conditions.  The `ObservatoryEnvironment` has a method called `add_dataset()` that creates a new temporary Google Cloud Dataset ID for the duration of the environment.

Note that if your test crashes (e.g., if it threw an uncaught exception), then the dataset will not be cleaned up afterwards, and will need to be manually removed, or removed as part of a routine dataset ID clearing.

### Observatory Platform API

Some telescopes make use of the Observatory Platform API in order to fetch necessary metadata.

The ObservatoryEnvironment supports creating an API endpoint that you can populate with test data. Example:
```
from airflow.models.connection import Connection
import observatory.api.server.orm as orm

env = ObservatoryEnvironment()
conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@host:port")
env.add_connection(conn)

dt = pendulum.utcnow()
telescope_type = orm.TelescopeType(name="ONIX Telescope", type_id=TelescopeTypes.onix, created=dt, modified=dt)
env.api_session.add(telescope_type)
organisation = orm.Organisation(name="Curtin Press", created=dt, modified=dt)
env.api_session.add(organisation)
telescope = orm.Telescope(
            name="Curtin Press ONIX Telescope",
            telescope_type=telescope_type,
            organisation=organisation,
            modified=dt,
            created=dt,
        )
env.api_session.add(telescope)
env.api_session.commit()
```
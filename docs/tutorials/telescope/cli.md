# Generating a new telescope using the CLI
The observatory cli tool can be used to generate a new telescope using one of the existing templates. 
To do this, use the command:
```shell script
observatory generate telescope <type> <class_name> "<Firstname> <Lastname>"
```

Where the type can be `Telescope`, `StreamTelescope`, `SnapshotTelescope` or `OrganisationTelescope`.
The class_name is the class name of the new telescope and the Firstname and Lastname are used for the author name
 that is included in the generated files.  
This command will generate all files that are required to add a new telescope and are mentioned above.

For example:
```shell script
observatory generate telescope SnapshotTelescope MyNewTelescope
```

Creates the following new files:
 * `observatory-dags/observatory/dags/dags/my_new_telescope.py`
 * `observatory-dags/observatory/dags/telescopes/my_new_telescope.py`
 * `tests/observatory/dags/telescopes/tests_my_new_telescope.py`
 * `docs/telescopes/my_new_telescope.md`
 * `observatory-dags/observatory/dags/database/schema/my_new_telescope_2021-08-01.json`

Updates the index file for the telescope documentation:
 * `docs/telescopes/index.rst`
 
And updates the TelescopeTypes in the identifiers file in case the new telescope is an OrganisationTelescope type:
 * `observatory-api/observatory/api/client/identifiers.py` 
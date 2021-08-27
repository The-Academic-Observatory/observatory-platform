# Generating a new workflows project using the CLI
#TODO


# Generating a new workflow using the CLI
The observatory cli tool can be used to generate a new workflow using one of the existing templates. 
To do this, use the command:
```shell script
observatory generate workflow <type> <class_name>
```

Where the type can be one of `Workflow`, `StreamTelescope`, `SnapshotTelescope` or `OrganisationTelescope` and
 the class_name is the class name of the new workflow.
 
This command has to be called either from inside a valid workflows project, or with the '-p/--project-path' 
 parameter specifying the path to a valid workflows project.  
A valid workflows project has the correct folder structure and includes a corresponding installed python package, a
 new project can be created with the `observatory generate project` command which is described above.  
The `observatory generate workflow` command will generate all files that are required to add a new workflow, see the
 example below.

For example:
```shell script
# Package inside "my-dags" is called "dags_package"
observatory generate workflow SnapshotTelescope MyNewWorkflow -p "my-dags"
```

Creates the following new files:
 * `my-dags/docs/my_new_workflow.md`
 * `my-dags/dags_package/dags/my_new_workflow.py`
 * `my-dags/dags_package/database/schema/my_new_workflow_2021-08-01.json`
 * `my-dags/dags_package/workflows/my_new_workflow.py`
 * `my-dags/tests/workflows/test_my_new_workflow.py`

Updates the index file for the workflow documentation:
 * `my-dags/docs/index.rst`
 
And updates the TelescopeTypes in the identifiers file in case the new workflow is an OrganisationTelescope type:
 * `my-dags/dags_package/utils/identifiers.py` 
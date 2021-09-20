# Generating a new workflows project using the CLI
The observatory cli tool can be used to generate a new workflows project.  
To do this, use the command:
 ```shell script
observatory generate project <project_path> <package_name> "Author Name"
```

This command will create a new directory at the <project_path> location and will create all required directories and
 files that are needed to use the new workflows project in this directory.  
This includes two setup files that are configured to install the <package_name> package, the installation of this
 package is optionally done as part of the `generate project` command.
The author name is only used for the readthedocs configuration, which is set up automatically.  

For example:
```shell script
observatory generate project /path/to/my-workflows-project my_workflows_project "Full Name"
```

Creates the following files and directories:
```
└── my-workflows-project
    ├── docs
    │   ├── _build
    │   ├── _static
    │   ├── _templates
    │   ├── workflows
    │   ├── generate_schema_csv.py
    │   ├── index.rst
    │   ├── make.bat
    │   └── Makefile
    ├── my_workflows_project
    │   ├── __init__.py
    │   ├── dags
    │   │   └── __init__.py
    │   ├── database
    │   │   ├── __init__.py
    │   │   └── schema
    │   │       └── __init__.py
    │   ├── utils
    │   │   └── __init__.py
    │   └── workflows
    │       └── __init__.py
    └── tests
        ├── __init__.py
        ├── setup.cfg
        ├── setup.py
        └── workflows
            └── __init__.py
```

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
# Package inside "my-dags" is called "my_dags"
observatory generate workflow SnapshotTelescope MyNewWorkflow -p "my-dags"
```

Creates the following new files:
 * `my-dags/docs/workflows/my_new_workflow.md`
 * `my-dags/my_dags/dags/my_new_workflow.py`
 * `my-dags/my_dags/database/schema/my_new_workflow_2021-08-01.json`
 * `my-dags/my_dags/workflows/my_new_workflow.py`
 * `my-dags/tests/workflows/test_my_new_workflow.py`

Updates the index file for the workflow documentation:
 * `my-dags/docs/index.rst`
 
And updates the TelescopeTypes in the identifiers file in case the new workflow is an OrganisationTelescope type:
 * `my-dags/my_dags/utils/identifiers.py` 
 
## License information
```eval_rst
The observatory-platform project itself has the :doc:`Apache License </license>` as specified.  
This means that all templates that are included in the observatory-platform project are subject to this License.  
However, the actual files that are generated with the templates and all code within those files are not subject to this
 License.  
```

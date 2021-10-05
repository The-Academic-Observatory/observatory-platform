import os
import shutil
import unittest

import docker
import docker.errors
import stringcase
from click.testing import CliRunner

from observatory.platform.docker.platform_builder import PlatformBuilder
from observatory.platform.observatory_config import (
    ObservatoryConfig,
    Backend,
    Observatory,
    WorkflowsProject,
    Environment,
    BackendType,
    module_file_path,
)
from observatory.platform.utils.test_utils import random_id, build_sdist, test_fixtures_path


class TestPlatformBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.tag_prefix = "the-academic-observatory"  # must be lower case
        self.observatory_api_path = module_file_path("observatory.api", nav_back_steps=-3)
        self.observatory_api_package_name = "observatory-api"
        self.observatory_platform_path = module_file_path("observatory.platform", nav_back_steps=-3)
        self.observatory_platform_package_name = "observatory-platform"
        self.workflows_package_name = "my-workflows-project"

    def test_build(self):
        # Make tag name
        tag = f"{self.tag_prefix}/{random_id()}:latest"
        try:
            runner = CliRunner()
            with runner.isolated_filesystem() as t:
                # Copy projects
                shutil.copytree(self.observatory_api_path, os.path.join(t, self.observatory_api_package_name))
                shutil.copytree(self.observatory_platform_path, os.path.join(t, self.observatory_platform_package_name))
                shutil.copytree(
                    test_fixtures_path("cli", self.workflows_package_name), os.path.join(t, self.workflows_package_name)
                )

                # Build sdists
                observatory_api_sdist_path = build_sdist(os.path.join(t, self.observatory_api_package_name))
                observatory_sdist_path = build_sdist(os.path.join(t, self.observatory_platform_package_name))
                workflows_sdist_path = build_sdist(os.path.join(t, self.workflows_package_name))

                # Make config
                backend = Backend(type=BackendType.build, environment=Environment.production)
                observatory = Observatory(
                    package=observatory_sdist_path,
                    package_type="sdist",
                    api_package=observatory_api_sdist_path,
                    api_package_type="sdist",
                )
                workflows_projects = [
                    WorkflowsProject(
                        package_name=self.workflows_package_name,
                        package=workflows_sdist_path,
                        package_type="sdist",
                        dags_module=f"{stringcase.snakecase(self.workflows_package_name)}.dags",
                    )
                ]
                config = ObservatoryConfig(
                    backend=backend, observatory=observatory, workflows_projects=workflows_projects
                )

                # Build image
                builder = PlatformBuilder(config=config, tag=tag)
                result = builder.build()
                self.assertTrue(result)
        finally:
            # Remove image if it exists
            try:
                docker.from_env().api.remove_image(tag, force=True)
            except docker.errors.ImageNotFound:
                pass

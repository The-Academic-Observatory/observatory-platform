import os
import shutil
from typing import Dict

from observatory.platform.docker.compose import ComposeRunner, ProcessOutput
from observatory.platform.utils.config_utils import module_file_path


class ElasticEnvironment(ComposeRunner):
    COMPOSE_FILE_NAME = "docker-compose.yml.jinja2"
    ELASTICSEARCH_FILE_NAME = "elasticsearch.yml"

    def __init__(self, build_path: str, elastic_port: int = 9200, kibana_port: int = 5601):
        self.elastic_module_path = module_file_path("observatory.platform.elastic")
        self.elasticsearch_config_path = os.path.join(self.elastic_module_path, self.ELASTICSEARCH_FILE_NAME)
        super().__init__(
            compose_file_path=os.path.join(self.elastic_module_path, self.COMPOSE_FILE_NAME),
            build_path=build_path,
            compose_args={"elastic_port": elastic_port, "kibana_port": kibana_port},
            debug=True,
        )

    def make_environment(self) -> Dict:
        return os.environ.copy()

    def start(self) -> ProcessOutput:
        os.makedirs(self.build_path, exist_ok=True)
        shutil.copy(self.elasticsearch_config_path, os.path.join(self.build_path, self.ELASTICSEARCH_FILE_NAME))
        self.stop()
        return super().start()

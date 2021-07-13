import logging
import os
import shutil
import time
from typing import Dict

import requests
from elasticsearch import Elasticsearch

from observatory.platform.docker.compose import ComposeRunner, ProcessOutput
from observatory.platform.utils.config_utils import module_file_path


class ElasticEnvironment(ComposeRunner):
    COMPOSE_FILE_NAME = "docker-compose.yml.jinja2"
    ELASTICSEARCH_FILE_NAME = "elasticsearch.yml"

    def __init__(
        self,
        build_path: str,
        elastic_port: int = 9200,
        kibana_port: int = 5601,
        wait: bool = True,
        wait_time_secs: int = 30,
    ):
        self.elastic_module_path = module_file_path("observatory.platform.elastic")
        self.elasticsearch_config_path = os.path.join(self.elastic_module_path, self.ELASTICSEARCH_FILE_NAME)
        self.wait = wait
        self.wait_time_secs = wait_time_secs
        self.elastic_uri = f"http://localhost:{elastic_port}/"
        self.kibana_uri = f"http://localhost:{kibana_port}/"
        super().__init__(
            compose_file_path=os.path.join(self.elastic_module_path, self.COMPOSE_FILE_NAME),
            build_path=build_path,
            compose_args={"elastic_port": elastic_port, "kibana_port": kibana_port},
            debug=True,
        )

        # Stop the awful unnecessary Elasticsearch connection warnings being logged
        logging.basicConfig()
        logging.getLogger().setLevel(logging.ERROR)

    def make_environment(self) -> Dict:
        return os.environ.copy()

    def start(self) -> ProcessOutput:
        os.makedirs(self.build_path, exist_ok=True)
        shutil.copy(self.elasticsearch_config_path, os.path.join(self.build_path, self.ELASTICSEARCH_FILE_NAME))
        self.stop()
        process_output = super().start()
        if self.wait:
            self.wait_until_started()
        return process_output

    def kibana_ping(self):
        try:
            response = requests.get(self.kibana_uri)
            return response.status_code == 200
        except (ConnectionResetError, requests.exceptions.ConnectionError):
            pass
        return False

    def wait_until_started(self):
        es = Elasticsearch([self.elastic_uri])
        start = time.time()
        services_found = False
        while True:
            elastic_found = es.ping()
            kibana_found = self.kibana_ping()
            services_found = elastic_found and kibana_found
            if services_found:
                break

            # Break out if time lasts too long
            elapsed = time.time() - start
            if elapsed > self.wait_time_secs:
                break

        return services_found
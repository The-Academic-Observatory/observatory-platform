def gcloud_builds_submit(self) -> Tuple[str, str, int]:
    # Build the google container image
    project_id = self.config.google_cloud.project_id
    # --gcs-logs-dir is specified to avoid storage.objects.get access error, see:
    # https://github.com/google-github-actions/setup-gcloud/issues/105
    # the _cloudbuild bucket is created already to store the build image
    args = [
        "gcloud",
        "builds",
        "submit",
        "--tag",
        f"gcr.io/{project_id}/observatory-api",
        "--project",
        project_id,
        "--gcs-log-dir",
        f"gs://{project_id}_cloudbuild/logs",
    ]
    if self.debug:
        print("Executing subprocess:")
        print(indent(f"Command: {subprocess.list2cmdline(args)}", INDENT1))
        print(indent(f"Cwd: {self.api_package_path}", INDENT1))

    proc: Popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=self.api_package_path)

    # Wait for results
    # Debug always true here because otherwise nothing gets printed and you don't know what the state of the
    # image building is
    output, error = stream_process(proc, True)

    info_filepath = os.path.join(self.terraform_build_path, "api_image_build.txt")
    with open(info_filepath, "w") as f:
        f.writelines(line + "\n" for line in output.splitlines()[-2:])
    return output, error, proc.returncode


# Copyright 2021 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: James Diprose

import os

import docker


def build_docker_file(context: str, tag: str, dockerfile: str = "Dockerfile") -> bool:
    """ Build a Docker file.

    :param context: the docker build context.
    :param tag: the Docker tag name.
    :param dockerfile: the Docker file name.
    :return: whether the build was successful or not.
    """

    client = docker.from_env()
    success = False
    for line in client.api.build(path=context, dockerfile=dockerfile, rm=True, decode=True, tag=tag):
        if "stream" in line:
            stream = line["stream"].rstrip()
            print(stream)
        elif "error" in line:
            error = line["error"].rstrip()
            print(error)
            success = False
        elif "aux" in line:
            success = True

    return success

# Copyright 2020 Google, LLC.
# Copyright 2021 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Use the official lightweight Python image.
# https://hub.docker.com/_/python

FROM python:3.9-slim

# Environment variables
ENV PBR_VERSION 0.0.1
ENV ES_API_KEY ""
ENV ES_HOST ""
ENV OBSERVATORY_DB_URI sqlite://
# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy observatory-api to the container image.
RUN mkdir /observatory-api
COPY . ./observatory-api

# Setup correct requirements
WORKDIR /observatory-api
RUN mv -f requirements.cloudrun.txt requirements.txt

# Install observatory-api
RUN pip3 install .

# Install berglas
COPY --from=gcr.io/berglas/berglas:latest /bin/berglas /bin/berglas

# Run app
ENTRYPOINT ["/bin/berglas", "exec",  "--", "gunicorn", "-b", "0.0.0.0:8080", "--timeout", "0", "observatory.api.server.app:app"]
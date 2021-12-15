#!/bin/bash

export PATH=$PATH:~/bin/openapitools/
export OPENAPI_GENERATOR_VERSION=5.3.0

if ! command -v openapi-generator-cli &> /dev/null
then
  mkdir -p ~/bin/openapitools
  curl https://raw.githubusercontent.com/OpenAPITools/openapi-generator/master/bin/utils/openapi-generator-cli.sh > ~/bin/openapitools/openapi-generator-cli
  chmod u+x ~/bin/openapitools/openapi-generator-cli
  openapi-generator-cli version
fi

if ! command -v observatory-api &> /dev/null
then
  pip install -e observatory-api
fi

# Directories
api_dir=observatory-api/observatory/api
server_dir=${api_dir}/server
client_dir=${api_dir}/client
docs_dir=docs/api

# Generate OpenAPI specification
observatory-api generate-openapi-spec ${server_dir}/openapi.yaml.jinja2 observatory-api/openapi.yaml --usage-type openapi_generator
cp observatory-api/openapi.yaml docs/api/openapi.yaml

# Generate OpenAPI Python client
openapi-generator-cli generate -i observatory-api/openapi.yaml -g python -c observatory-api/api-config.yaml -t observatory-api/templates/ -o observatory-api

# Massage files into correct directory
mv ${api_dir}/client_README.md ${docs_dir}/api_client.md
cp -rn ${client_dir}/test/* ${api_dir}/tests/client/
mv ${client_dir}/docs/* ${docs_dir}
rm -r ${client_dir}/test/ ${client_dir}/docs/ ${client_dir}/apis/ ${client_dir}/models/
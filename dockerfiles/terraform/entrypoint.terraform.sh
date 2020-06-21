#!/bin/sh

TOKEN=$(cat ${TERRAFORM_APP_TOKEN})
sed -i -e 's/terraform_app_token/'"${TOKEN}"'/g' $HOME/.terraform.d/credentials.tfrc.json

cd ${TERRAFORM_WORKSPACES}
exec "$@"
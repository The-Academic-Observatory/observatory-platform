# API Server
This is a tutorial for deploying the Observatory Platform to Google Cloud with Terraform.

You should have [installed the Observatory Platform](installation.html) before following this tutorial.

## How to deploy
The API is part of the Terraform configuration and is deployed when the Terraform configuration is applied.
See the 'Observatory Terraform Environment' section for more information on how to do this. 
The URL of the API corresponds to the URL in the 'endpoints' Cloud Run service.

## Generating an API key
To generate an API key, in the Google Cloud Console go to 'APIs & Services' -> 'Credentials' and click 'Create Credentials' in the top bar.
From the dropdown menu select, 'API key'. The API key is then generated for you. 
For security reasons, restrict the API key to only the 'Observatory API' service.

## Adding a user to the API
From the Google Cloud Endpoints page, click on "Observatory API" and then click "ADD MEMBER" at the far right of the
screen. Add the email address of the user that you want to add and give them the Role "Service Consumer".
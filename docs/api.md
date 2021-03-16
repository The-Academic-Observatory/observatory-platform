# Observatory API
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

## Usage examples
Example URL: https://endpoints-98d9421ddec0df30-o6ogbcoyxq-uw.a.run.app

https://endpoints-98d9421ddec0df30-o6ogbcoyxq-uw.a.run.app/query?agg=institution&subset=journals&from=2018&to=2019&journal=Molecular Pharmaceutics&key=API_KEY
https://endpoints-98d9421ddec0df30-o6ogbcoyxq-uw.a.run.app/query?agg=country&subset=oa-metrics&index_date=20201212&key=API_KEY
https://endpoints-98d9421ddec0df30-o6ogbcoyxq-uw.a.run.app/query?agg=institution&subset=journals&id=grid.4691.a,grid.469280.1&key=API_KEY
https://endpoints-98d9421ddec0df30-o6ogbcoyxq-uw.a.run.app/query?agg=country&subset=collaborations&key=API_KEY
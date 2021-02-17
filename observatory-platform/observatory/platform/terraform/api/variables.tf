variable "google_cloud" {
  description = <<EOF
The Google Cloud settings for the Observatory Platform.

project_id: the Google Cloud project id.
credentials: the path to the Google Cloud credentials.
region: the Google Cloud region.
zone: the Google Cloud zone.
data_location: the data location for storing buckets.
EOF
  type = object({
    project_id = string
    credentials = string
    region = string
    zone = string
    data_location = string
  })
}

variable "elasticsearch" {
    description = <<EOF
Elasticsearch login information

api_key: The elasticsearch api key
host: The address of the elasticsearch server
EOF
  type = object({
    api_key = string
    host = string
  })
}
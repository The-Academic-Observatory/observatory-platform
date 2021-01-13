project = "waypoint-elasticsearch-project"

app "waypoint-elasticsearch-app" {
  labels = {
    "service" = "waypoint-elasticsearch-label",
    "env"     = "dev"
  }

  build {
    use "docker" {
      dockerfile = "DockerfileWaypoint"
    }

    registry {
      use "docker" {
        image = "gcr.io/workflows-dev/waypoint-elasticsearch-image"
        tag   = "latest"
      }
    }
  }

  deploy {
    use "google-cloud-run" {
      project  = "workflows-dev"
      location = "us-east1"

      port = 8080

      static_environment = {
        "ES_USERNAME": "sm://workflows-dev/es_username"
        "ES_PASSWORD": "sm://workflows-dev/es_password"
        "ES_ADDRESS": "sm://workflows-dev/es_url"
      }

      capacity {
        memory                     = 128
        cpu_count                  = 1
        max_requests_per_container = 10
        request_timeout            = 300
      }

//      service_account_name = "elasticsearch-api@workflows-dev.iam.gserviceaccount.com"

      auto_scaling {
        max = 2
      }
    }
  }

  release {
    use "google-cloud-run" {}
  }
}

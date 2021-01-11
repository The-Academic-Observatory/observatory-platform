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
        "NAME": "World"
      }

      capacity {
        memory                     = 128
        cpu_count                  = 1
        max_requests_per_container = 10
        request_timeout            = 300
      }

      auto_scaling {
        max = 2
      }
    }
  }

  release {
    use "google-cloud-run" {}
  }
}

# Create Compute Network
resource "google_compute_network" "compute_network" {
  count = var.status == "on" ? 1 : 0
  name                    = "composer-network-dev"
  auto_create_subnetworks = false
}
# Create subnetwork
resource "google_compute_subnetwork" "compute_subnetwork" {
  count = var.status == "on" ? 1 : 0
  name          = "composer-subnetwork-dev"
  ip_cidr_range = "10.2.0.0/16"
  region        = var.region_
  network       = google_compute_network.compute_network[count.index].id
}

resource "google_composer_environment" "cloud_composer" {
  count = var.status == "on" ? 1 : 0
  name   = var.composer_name
  region = var.region_

  config {
    # has to be at least 3
    node_count = 3

    software_config {
      python_version = 3
      # Same as airflow version that is used in local development docker image
      image_version = "composer-1.10.2-airflow-1.10.6"

      env_variables = {
        GITHUB_BUCKET_PATH = var.github_bucket_path
        COMPOSER_ENVIRONMENT_NAME = var.composer_name
        COMPOSER_LOCATION = var.region_
        # This is used by observatory_home() in utils_config.py
        OBSERVATORY_PATH = "/home/airflow/gcs/data"
      }
      # easy to create list of packages with awk statement:
      # cat requirements.txt | awk '{if ($0 !~ /google|airflow|github.com/){if ($0 ~ "=="){split($0,a,"=="); printf"%s = \"==%s%s\"\n", tolower(a[1]), a[2], a[3]} else if($0 ~ ">="){split($0,b,">="); printf"%s = \">=%s%s\"\n", tolower(b[1]), b[2], b[3]} else if($0 ~ "<="){split($0,c,"<="); printf"%s = \">=%s%s\"\n", tolower(c[1]), c[2], c[3]}}}'
      # TODO get sickle from pivate repo
      pypi_packages = {
        ray = "==0.8.*"
        lxml = "==4.4.*"
        beautifulsoup4 = "==4.8.*"
        validators = "==0.14.*"
        pandas = "==0.25.*"
        natsort = "==7.0.*"
        psutil = "==5.6.*"
        tldextract = "==2.2.*"
        setproctitle = "==1.1.*"
        requests = "==2.23.*"
        urllib3 = "==1.25.*"
        numpy = "==1.18.*"
        certifi = ">=2019.11.28"
        python-dateutil = "==2.8.*"
        requests-file = "==1.4.*"
        six = "==1.13.*"
        boto3 = "==1.10.*"
        warcio = "==1.7.*"
        httpretty = "==0.9.*"
        aiohttp = "==3.6.*"
        grpcio = "==1.26.*"
        matplotlib = "==3.1.*"
        jupyter = "==1.0.*"
        seaborn = "==0.10.*"
        click = "==7.1.*"
        docker-compose = "==1.25.*"
        cerberus = "==1.3.*"
      }
    }
    node_config {
      zone = var.zone_
      machine_type = var.machine-type
      disk_size_gb = var.composer-disk-size-gb

      network = google_compute_network.compute_network[count.index].id
      subnetwork = google_compute_subnetwork.compute_subnetwork[count.index].id
    }
  }
}
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.12.0"
    }
  }
}

provider "google" {
  credentials = "prismatic-grail-420403-9430c324bd9a.json"
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "hw-bucket" {
  name          = "prismatic-grail-420403-homework-bucket"
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "hw-dataset" {
  dataset_id = var.bq_dataset_name
  location = var.location
}
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file("brasileirao-arbitragem-c713c5f62e2b.json")
  project     = var.project_id
  region      = var.region
}

resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  force_destroy = true
}

resource "google_bigquery_dataset" "staging" {
  dataset_id = "staging"
  location   = var.region
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "analytics"
  location   = var.region
}

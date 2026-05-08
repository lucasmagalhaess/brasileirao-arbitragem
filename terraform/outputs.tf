output "bucket_name" {
  value = google_storage_bucket.data_lake.name
}

output "staging_dataset" {
  value = google_bigquery_dataset.staging.dataset_id
}

output "analytics_dataset" {
  value = google_bigquery_dataset.analytics.dataset_id
}

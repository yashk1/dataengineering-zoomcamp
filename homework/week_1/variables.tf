variable "project" {
  description = "Project ID"
  default = "prismatic-grail-420403"
}

variable "region" {
  description = "Region"
  default = "us-west1-a"
}

variable "location" {
  description = "Project Location"
  default = "US"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default = "STANDARD"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default = "hw_dataset"
}
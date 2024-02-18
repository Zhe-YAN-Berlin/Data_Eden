variable "credentials" {
  description = "value"
  default     = "./my-creds.json"
}


variable "project" {
  description = "project"
  default     = "terraform-demo-414613"
}


variable "location" {
  description = "project location"
  default     = "US"
}


variable "bq_dataset_name" {
  description = "my bigquery dataset name"
  default     = "demo_dataset"
}


variable "gcs_bucket_name" {
  description = "my storage bucket name"
  default     = "terraform-demo-414613"
}


variable "gcs_storage_class" {
  description = "bucket storage class"
  default     = "STANDARD"
}

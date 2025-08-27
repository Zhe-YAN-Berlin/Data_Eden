variable "credentials" {
  description = "value"
  default     = "./vm-service-acc-key.json"
}


variable "project" {
  description = "project"
  default     = "my-zhe-414813"
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
  default     = "my-zhe-414813"
}


variable "gcs_storage_class" {
  description = "bucket storage class"
  default     = "STANDARD"
}

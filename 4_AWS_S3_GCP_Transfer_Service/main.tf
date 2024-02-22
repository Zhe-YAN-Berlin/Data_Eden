terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.16.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = "us-central1"
}


data "google_storage_transfer_project_service_account" "default" {
  project = var.project
}

resource "google_storage_bucket" "s3-backup-bucket" {
  name          = "${var.aws_s3_bucket}-backup"
  storage_class = "NEARLINE"
  project       = var.project
  location      = "US"
}

resource "google_storage_bucket_iam_member" "s3-backup-bucket" {
  bucket     = google_storage_bucket.s3-backup-bucket.name
  role       = "roles/storage.admin"
  member     = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
  depends_on = [google_storage_bucket.s3-backup-bucket]
}



resource "google_storage_transfer_job" "s3-bucket-nightly-backup" {
  description = "Nightly backup of S3 bucket"
  project     = var.project

  transfer_spec {
    object_conditions {
      max_time_elapsed_since_last_modification = "600s"
      exclude_prefixes = [
        "requests.gz",
      ]
    }
    transfer_options {
      delete_objects_unique_in_sink = false
    }
    aws_s3_data_source {
      bucket_name = var.aws_s3_bucket
      aws_access_key {
        access_key_id     = var.aws_access_key
        secret_access_key = var.aws_secret_key
      }
    }
    gcs_data_sink {
      bucket_name = google_storage_bucket.s3-backup-bucket.name
      path        = "foo/bar/"
    }
  }

  schedule {
    schedule_start_date {
      year  = 2024
      month = 2
      day   = 22
    }
    schedule_end_date {
      year  = 2024
      month = 2
      day   = 22
    }

  }


  depends_on = [google_storage_bucket_iam_member.s3-backup-bucket]
}
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

variable "aws_s3_bucket" {
  description = "aws s3 bucket name"
  default     = "nyc-tlc"
}


variable "gcs_storage_class" {
  description = "bucket storage class"
  default     = "STANDARD"
}

variable "aws_access_key" {
  description = "aws access key"
  default     = "AKIAYS2NW7XXXX"
}

variable "aws_secret_key" {
  description = "aws secret key"
  default     = "XXXXXX"
}
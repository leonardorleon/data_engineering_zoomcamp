locals {
  data_lake_bucket = "dtc_data_lake"
}

# No default value provided, so it needs to be assigned at runtime
variable "project" {
  default     = "sunlit-amulet-341719"
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "EUROPE-WEST1"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "trips_data_all"
}
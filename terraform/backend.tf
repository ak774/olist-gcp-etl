
terraform {
  backend "gcs" {
    bucket = "olist-terraform-state-dev"
    prefix = "env/dev"
  }
}


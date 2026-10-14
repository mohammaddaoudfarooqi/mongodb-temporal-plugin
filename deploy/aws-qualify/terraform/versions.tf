# Qualification stack: one root module, local state (gitignored). One apply
# builds the AWS side and the Atlas side inside the operator's project,
# so the resource graph orders peering, routes and the access list.
terraform {
  required_version = ">= 1.13"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    mongodbatlas = {
      source  = "mongodb/mongodbatlas"
      version = "~> 2.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

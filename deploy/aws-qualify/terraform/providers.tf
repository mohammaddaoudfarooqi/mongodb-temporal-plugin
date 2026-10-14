provider "aws" {
  region = var.aws_region
  default_tags {
    tags = local.common_tags
  }
  # Tags that something outside Terraform adds after creation (an
  # organisation's auto-tagger, say); ignored so plans show only real drift.
  ignore_tags {
    key_prefixes = var.ignore_tag_prefixes
  }
}

# Project-scoped API key with Project Owner on atlas_project_id. Passed
# by qualify.sh as TF_VAR_atlas_public_key / TF_VAR_atlas_private_key.
provider "mongodbatlas" {
  public_key  = var.atlas_public_key
  private_key = var.atlas_private_key
}

# Account id: the peer VPC owner for the peering Atlas initiates.
data "aws_caller_identity" "current" {}

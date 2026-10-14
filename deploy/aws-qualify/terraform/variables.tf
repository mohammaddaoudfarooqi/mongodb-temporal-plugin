variable "run_id" {
  description = "Identifies this run; names and tags every resource."
  type        = string
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{2,19}$", var.run_id))
    error_message = "run_id must be 3-20 lowercase letters, digits or dashes."
  }
}

variable "profile" {
  description = "functional or load (release-qualification test stacks), or quickstart (a lasting deployment)."
  type        = string
  validation {
    condition     = contains(["functional", "load", "quickstart"], var.profile)
    error_message = "profile must be functional, load or quickstart."
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "availability_zone" {
  type    = string
  default = "us-east-1a"
}

variable "atlas_project_id" {
  description = "The operator's Atlas project. Never created or deleted here."
  type        = string
}

variable "atlas_public_key" {
  type      = string
  sensitive = true
}

variable "atlas_private_key" {
  type      = string
  sensitive = true
}

variable "owner_email" {
  type = string
}

variable "purpose" {
  type    = string
  default = "partners"
}

variable "expire_on" {
  description = "YYYY-MM-DD; qualify.sh sets it to the day after the run."
  type        = string
}

variable "admin_cidrs" {
  description = "IPv4 CIDRs allowed to SSH to the hosts: the operator's /32 plus any allowed ranges from .env."
  type        = list(string)
  validation {
    condition     = length(var.admin_cidrs) > 0 && length(var.admin_cidrs) <= 55
    error_message = "admin_cidrs needs 1 to 55 entries (the security group's rule limit, less the other rules)."
  }
  validation {
    condition     = alltrue([for c in var.admin_cidrs : can(cidrnetmask(c)) && tonumber(split("/", c)[1]) >= 8])
    error_message = "admin_cidrs entries must be IPv4 CIDRs no wider than /8; SSH is never opened to the world."
  }
}

variable "public_key_path" {
  description = "SSH public key imported as the hosts' key pair."
  type        = string
}

variable "vpc_cidr" {
  type    = string
  default = "10.40.0.0/16"
}

variable "subnet_cidr" {
  type    = string
  default = "10.40.1.0/24"
}

variable "atlas_cidr" {
  description = "CIDR for a new Atlas network container; a reused one keeps its own."
  type        = string
  default     = "192.168.248.0/21"
}

variable "atlas_instance_size" {
  description = "Atlas tier; empty uses the profile default (M10 / M30)."
  type        = string
  default     = ""
}

variable "atlas_disk_gb" {
  description = "Atlas disk size; 0 uses the profile default."
  type        = number
  default     = 0
}

variable "mongodb_version" {
  type    = string
  default = "8.0"
}

variable "server_count" {
  description = "Temporal server hosts; 0 uses the profile default (1 / 2)."
  type        = number
  default     = 0
}

variable "server_instance_type" {
  type    = string
  default = ""
}

variable "loadgen_instance_type" {
  type    = string
  default = ""
}

variable "max_hours" {
  description = "Hosts terminate themselves after this many hours (test profiles; the quickstart never does)."
  type        = number
  default     = 0
}

variable "ami_id" {
  description = "Pin an AMI; empty resolves the latest Amazon Linux 2023."
  type        = string
  default     = ""
}

variable "existing_container_id" {
  description = "The project's existing AWS network container in the region, found by qualify.sh before the first apply; empty creates one. Fixed for the life of the run so ownership never flips."
  type        = string
  default     = ""
}

variable "existing_container_cidr" {
  description = "CIDR of existing_container_id."
  type        = string
  default     = ""
}

variable "ignore_tag_prefixes" {
  description = "Tag key prefixes added outside Terraform (e.g. by an organisation's auto-tagger) that plans should ignore. Set in an untracked *.auto.tfvars file."
  type        = list(string)
  default     = []
}

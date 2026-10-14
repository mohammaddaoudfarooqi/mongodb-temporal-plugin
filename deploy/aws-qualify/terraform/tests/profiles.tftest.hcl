# Plan-time tests for the qualification module.
# Run: terraform -chdir=deploy/aws-qualify/terraform test
# Mock providers: no AWS or Atlas credentials are needed.

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
  }
  mock_data "aws_ami" {
    defaults = { id = "ami-0123456789abcdef0" }
  }
}
mock_provider "random" {}
mock_provider "mongodbatlas" {
  mock_resource "mongodbatlas_advanced_cluster" {
    defaults = {
      connection_strings = { standard_srv = "mongodb+srv://tmc-test.abcde.mongodb.net" }
    }
  }
}

variables {
  run_id            = "test-1"
  atlas_project_id  = "0123456789abcdef01234567"
  atlas_public_key  = "pub"
  atlas_private_key = "priv"
  owner_email       = "owner@example.com"
  expire_on         = "2026-10-14"
  admin_cidrs       = ["203.0.113.7/32", "198.51.100.0/29"]
  public_key_path   = "tests/fixture_key.pub"
}

run "functional_profile_is_minimal" {
  command = plan
  variables { profile = "functional" }
  assert {
    condition     = length(aws_instance.server) == 1 && length(aws_instance.loadgen) == 0
    error_message = "functional must plan one server host and no load generator"
  }
  assert {
    condition     = mongodbatlas_advanced_cluster.cluster.replication_specs[0].region_configs[0].electable_specs.instance_size == "M10"
    error_message = "functional must use an M10 cluster"
  }
}

run "load_profile_is_production_shaped" {
  command = plan
  variables { profile = "load" }
  assert {
    condition     = length(aws_instance.server) == 2 && length(aws_instance.loadgen) == 1
    error_message = "load must plan two server hosts and one load generator"
  }
  assert {
    condition     = mongodbatlas_advanced_cluster.cluster.replication_specs[0].region_configs[0].electable_specs.instance_size == "M30"
    error_message = "load must default to M30"
  }
}

run "load_profile_overrides" {
  command = plan
  variables {
    profile             = "load"
    server_count        = 3
    atlas_instance_size = "M50"
  }
  assert {
    condition     = length(aws_instance.server) == 3
    error_message = "server_count must override the profile default"
  }
  assert {
    condition     = mongodbatlas_advanced_cluster.cluster.replication_specs[0].region_configs[0].electable_specs.instance_size == "M50"
    error_message = "atlas_instance_size must override the profile default"
  }
}

run "atlas_resources_target_given_project" {
  command = plan
  variables { profile = "functional" }
  assert {
    condition = alltrue([
      mongodbatlas_advanced_cluster.cluster.project_id == var.atlas_project_id,
      mongodbatlas_database_user.qualify.project_id == var.atlas_project_id,
      mongodbatlas_network_peering.aws.project_id == var.atlas_project_id,
    ])
    error_message = "every Atlas resource must target atlas_project_id"
  }
}

run "security_group_admits_only_ssh_and_itself" {
  command = plan
  variables { profile = "load" }
  assert {
    condition     = length(aws_security_group.hosts.ingress) == 2
    error_message = "exactly two ingress rules: SSH from admin_cidrs and the group itself"
  }
  assert {
    condition = anytrue([for r in aws_security_group.hosts.ingress :
    r.from_port == 22 && r.to_port == 22 && r.protocol == "tcp" && toset(r.cidr_blocks) == toset(["203.0.113.7/32", "198.51.100.0/29"])])
    error_message = "SSH must be admitted from exactly the admin_cidrs"
  }
  assert {
    condition = anytrue([for r in aws_security_group.hosts.ingress :
    r.self == true && length(coalesce(r.cidr_blocks, [])) == 0])
    error_message = "the group must admit traffic from itself, with no CIDR"
  }
}

run "access_list_is_vpc_only" {
  command = plan
  variables { profile = "functional" }
  assert {
    condition     = mongodbatlas_project_ip_access_list.vpc.cidr_block == "10.40.0.0/16"
    error_message = "the access list must contain only the VPC CIDR"
  }
}

run "creates_container_when_none_exists" {
  command = plan
  variables { profile = "functional" }
  assert {
    condition     = length(mongodbatlas_network_container.aws) == 1
    error_message = "a container must be created when the project has none for the region"
  }
}

run "reuses_existing_container" {
  command = plan
  variables {
    profile                 = "functional"
    existing_container_id   = "existing-ctr"
    existing_container_cidr = "192.168.240.0/21"
  }
  assert {
    condition     = length(mongodbatlas_network_container.aws) == 0 && mongodbatlas_network_peering.aws.container_id == "existing-ctr"
    error_message = "an existing container for the region must be reused"
  }
  assert {
    condition     = aws_route.to_atlas.destination_cidr_block == "192.168.240.0/21"
    error_message = "the route must target the reused container's CIDR"
  }
}

run "hosts_are_tagged_hardened_and_capped" {
  command = plan
  variables {
    profile   = "load"
    max_hours = 5
  }
  assert {
    condition = alltrue([for i in concat(aws_instance.server, aws_instance.loadgen) :
    i.metadata_options[0].http_tokens == "required" && i.root_block_device[0].encrypted])
    error_message = "hosts must require IMDSv2 and encrypt the root volume"
  }
  assert {
    condition = alltrue([for i in concat(aws_instance.server, aws_instance.loadgen) :
    i.instance_initiated_shutdown_behavior == "terminate" && strcontains(i.user_data, "shutdown -h +300")])
    error_message = "hosts must terminate themselves after max_hours"
  }
  assert {
    condition = alltrue([for k, v in {
      "qualify-run" = "test-1", owner = "owner@example.com", purpose = "partners", "expire-on" = "2026-10-14"
    } : local.common_tags[k] == v])
    error_message = "common tags must carry the run id, owner, purpose and expiry"
  }
}

run "uri_is_sensitive_and_not_in_user_data" {
  command = plan
  variables { profile = "functional" }
  assert {
    condition     = alltrue([for i in aws_instance.server : !strcontains(i.user_data, "mongodb")])
    error_message = "user data must not contain the connection string"
  }
}

run "rejects_unknown_profile" {
  command = plan
  variables { profile = "huge" }
  expect_failures = [var.profile]
}

# Container ownership comes from inputs that qualify.sh
# fixes before the first apply and reuses for every retry and destroy.
# Nothing read at plan time can flip it, so a re-plan after apply keeps
# the container this stack created.
run "first_apply_creates_container" {
  command = apply
  variables { profile = "functional" }
  assert {
    condition     = length(mongodbatlas_network_container.aws) == 1
    error_message = "the first apply must create the container"
  }
}

run "replan_after_apply_keeps_owned_container" {
  command = plan
  variables { profile = "functional" }
  assert {
    condition     = length(mongodbatlas_network_container.aws) == 1
    error_message = "a re-plan must keep the container this stack created"
  }
}

run "rejects_invalid_admin_cidr" {
  command = plan
  variables {
    profile     = "functional"
    admin_cidrs = ["203.0.113.7/32", "not-a-cidr"]
  }
  expect_failures = [var.admin_cidrs]
}

run "rejects_world_open_ssh" {
  command = plan
  variables {
    profile     = "functional"
    admin_cidrs = ["0.0.0.0/0"]
  }
  expect_failures = [var.admin_cidrs]
}

# The AWS quickstart: a lasting deployment, not a test stack.
run "quickstart_profile_is_a_lasting_deployment" {
  command = plan
  variables { profile = "quickstart" }
  assert {
    condition     = length(aws_instance.server) == 1 && length(aws_instance.loadgen) == 0
    error_message = "quickstart must plan one host and no load generator"
  }
  assert {
    condition     = mongodbatlas_advanced_cluster.cluster.replication_specs[0].region_configs[0].electable_specs.instance_size == "M10"
    error_message = "quickstart must default to M10"
  }
  assert {
    condition     = mongodbatlas_advanced_cluster.cluster.replication_specs[0].region_configs[0].auto_scaling.disk_gb_enabled == true
    error_message = "quickstart needs storage auto-scaling (Atlas automated embedding requires it)"
  }
  assert {
    condition = alltrue([for i in aws_instance.server :
    !strcontains(i.user_data, "shutdown -h") && i.instance_initiated_shutdown_behavior == "stop"])
    error_message = "quickstart hosts must not terminate themselves"
  }
}

run "test_profiles_keep_the_self_terminate_cap" {
  command = plan
  variables { profile = "functional" }
  assert {
    condition = alltrue([for i in aws_instance.server :
    strcontains(i.user_data, "shutdown -h") && i.instance_initiated_shutdown_behavior == "terminate"])
    error_message = "test stacks must keep the self-terminate cap"
  }
}

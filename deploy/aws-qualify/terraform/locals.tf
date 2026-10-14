locals {
  defaults = {
    functional = { atlas = "M10", disk = 10, servers = 1, loadgen = 0, server_type = "c6i.large", loadgen_type = "c6i.large", max_hours = 3, disk_autoscale = false }
    load       = { atlas = "M30", disk = 40, servers = 2, loadgen = 1, server_type = "c6i.2xlarge", loadgen_type = "c6i.xlarge", max_hours = 8, disk_autoscale = false }
    # A lasting deployment: no self-terminate; storage auto-scaling, which
    # Atlas automated embedding (vector search) requires.
    quickstart = { atlas = "M10", disk = 10, servers = 1, loadgen = 0, server_type = "c6i.large", loadgen_type = "c6i.large", max_hours = 0, disk_autoscale = true }
  }
  p = local.defaults[var.profile]

  atlas_size   = var.atlas_instance_size != "" ? var.atlas_instance_size : local.p.atlas
  atlas_disk   = var.atlas_disk_gb > 0 ? var.atlas_disk_gb : local.p.disk
  server_count = var.server_count > 0 ? var.server_count : local.p.servers
  server_type  = var.server_instance_type != "" ? var.server_instance_type : local.p.server_type
  loadgen_type = var.loadgen_instance_type != "" ? var.loadgen_instance_type : local.p.loadgen_type
  max_hours    = var.profile == "quickstart" ? 0 : (var.max_hours > 0 ? var.max_hours : local.p.max_hours)
  # Self-terminating test hosts terminate on shutdown; quickstart hosts stop.
  shutdown_behavior = local.max_hours > 0 ? "terminate" : "stop"
  loadgen_count     = local.p.loadgen

  name         = "tmc-${var.run_id}"
  atlas_region = upper(replace(var.aws_region, "-", "_"))

  # Atlas allows one network container per project, provider and region;
  # reuse the project's if it had one before this run. The
  # decision is an input, not a data source: after the first apply a
  # data source would also see the container this stack created and
  # plan its deletion.
  reuse          = var.existing_container_id != ""
  container_id   = local.reuse ? var.existing_container_id : mongodbatlas_network_container.aws[0].container_id
  atlas_cidr_eff = local.reuse ? var.existing_container_cidr : var.atlas_cidr

  common_tags = {
    Name          = local.name
    Project       = "mongodb-temporal-plugin-qualify"
    ManagedBy     = "terraform"
    "qualify-run" = var.run_id
    owner         = var.owner_email
    OwnerContact  = var.owner_email
    purpose       = var.purpose
    "expire-on"   = var.expire_on
  }
  # Atlas rejects blank tag values.
  atlas_tags = { for k, v in local.common_tags : k => v if v != "" }

  # standard_srv is "mongodb+srv://<host>"; the password is alphanumeric,
  # so no URL encoding is needed. Exposed only as a sensitive output.
  srv_host    = replace(mongodbatlas_advanced_cluster.cluster.connection_strings.standard_srv, "mongodb+srv://", "")
  mongodb_uri = "mongodb+srv://${mongodbatlas_database_user.qualify.username}:${random_password.db.result}@${local.srv_host}/?retryWrites=true&w=majority"
}

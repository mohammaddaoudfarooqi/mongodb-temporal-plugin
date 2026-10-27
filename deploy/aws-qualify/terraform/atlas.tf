# Atlas side, all inside the operator's project: cluster,
# database user and the VPC access list entry. Termination protection is
# off so destroy needs no console step.
resource "mongodbatlas_advanced_cluster" "cluster" {
  project_id                     = var.atlas_project_id
  name                           = local.name
  cluster_type                   = "REPLICASET"
  mongo_db_major_version         = var.mongodb_version
  termination_protection_enabled = false

  replication_specs = [
    {
      region_configs = [
        {
          electable_specs = {
            instance_size = local.atlas_size
            node_count    = 3
            disk_size_gb  = local.atlas_disk
          }
          auto_scaling = {
            disk_gb_enabled = local.p.disk_autoscale
          }
          provider_name = "AWS"
          priority      = 7
          region_name   = local.atlas_region
        }
      ]
    }
  ]

  # Atlas refuses a failover test below 3 hours of oplog capacity. With
  # 16 load-worker pollers the load suite can write about 20 GB of oplog
  # an hour (measured on M40), so the load profile sizes it for that.
  # 0 keeps Atlas's default.
  advanced_configuration = {
    oplog_size_mb = local.p.oplog_mb > 0 ? local.p.oplog_mb : null
  }

  tags = local.atlas_tags

  timeouts = {
    create = "40m"
    update = "40m"
    delete = "30m"
  }

  # Without this edge Atlas may create a default container for the
  # cluster first, and the explicit container then conflicts.
  depends_on = [mongodbatlas_network_container.aws]
}

resource "random_password" "db" {
  length  = 32
  special = false
}

# Roles cover every database the suites create (per-mode prefixes,
# gate databases), index and Atlas Search management, and serverStatus
# for soak snapshots. The cluster is dedicated to one run.
resource "mongodbatlas_database_user" "qualify" {
  project_id         = var.atlas_project_id
  username           = local.name
  password           = random_password.db.result
  auth_database_name = "admin"

  roles {
    role_name     = "readWriteAnyDatabase"
    database_name = "admin"
  }
  roles {
    role_name     = "dbAdminAnyDatabase"
    database_name = "admin"
  }
  roles {
    role_name     = "clusterMonitor"
    database_name = "admin"
  }

  scopes {
    name = mongodbatlas_advanced_cluster.cluster.name
    type = "CLUSTER"
  }
}

# Only the peered VPC may connect. Opened once the route
# to Atlas exists.
resource "mongodbatlas_project_ip_access_list" "vpc" {
  project_id = var.atlas_project_id
  cidr_block = var.vpc_cidr
  comment    = local.name

  depends_on = [aws_route.to_atlas]
}

# The quickstart's services run as this least-privilege user: readWrite on
# the Temporal and showcase databases only. The admin user above sets up
# the schema and indexes and is never stored on the host.
resource "random_password" "runtime" {
  count   = var.profile == "quickstart" ? 1 : 0
  length  = 32
  special = false
}

resource "mongodbatlas_database_user" "runtime" {
  count              = var.profile == "quickstart" ? 1 : 0
  project_id         = var.atlas_project_id
  username           = "${local.name}-runtime"
  password           = random_password.runtime[0].result
  auth_database_name = "admin"

  roles {
    role_name     = "readWrite"
    database_name = "temporal"
  }
  roles {
    role_name     = "readWrite"
    database_name = "support"
  }

  scopes {
    name = mongodbatlas_advanced_cluster.cluster.name
    type = "CLUSTER"
  }
}

# load: a production-shaped deployment. Override the tier
# with --atlas-tier and the server hosts with --servers.
atlas_instance_size   = "M30"
atlas_disk_gb         = 40
server_count          = 2
server_instance_type  = "c6i.2xlarge"
loadgen_instance_type = "c6i.xlarge"
max_hours             = 8

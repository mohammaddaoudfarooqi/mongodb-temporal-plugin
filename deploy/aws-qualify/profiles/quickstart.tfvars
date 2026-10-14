# quickstart: a lasting deployment of Temporal on Atlas, peered with one
# EC2 host. Storage auto-scaling is on (Atlas automated embedding needs
# it); the host never terminates itself.
atlas_instance_size  = "M10"
atlas_disk_gb        = 10
server_count         = 1
server_instance_type = "c6i.large"

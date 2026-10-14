output "run_id" { value = var.run_id }
output "profile" { value = var.profile }
output "cluster_name" { value = mongodbatlas_advanced_cluster.cluster.name }
output "atlas_tier" { value = local.atlas_size }
output "mongodb_version" { value = var.mongodb_version }
output "server_public_ips" { value = aws_instance.server[*].public_ip }
output "server_private_ips" { value = aws_instance.server[*].private_ip }
output "loadgen_public_ips" { value = aws_instance.loadgen[*].public_ip }
output "loadgen_private_ips" { value = aws_instance.loadgen[*].private_ip }
output "vpc_peering_connection_id" { value = mongodbatlas_network_peering.aws.connection_id }
output "atlas_cidr" { value = local.atlas_cidr_eff }
output "created_container" { value = !local.reuse }
output "srv_host" { value = local.srv_host }

# Delivered to the hosts over SSH stdin by qualify.sh, never printed
#.
output "mongodb_uri" {
  value     = local.mongodb_uri
  sensitive = true
}

# The setup-only credential (schema, indexes). Same as mongodb_uri on
# test stacks.
output "mongodb_admin_uri" {
  value     = local.admin_uri
  sensitive = true
}
output "vpc_id" { value = aws_vpc.main.id }

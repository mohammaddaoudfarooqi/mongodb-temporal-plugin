# AWS <-> Atlas VPC peering. Atlas initiates the request from its
# container; AWS accepts; a route sends the Atlas CIDR over the peering;
# the access list then admits the VPC CIDR.
resource "mongodbatlas_network_container" "aws" {
  count            = local.reuse ? 0 : 1
  project_id       = var.atlas_project_id
  atlas_cidr_block = var.atlas_cidr
  provider_name    = "AWS"
  region_name      = local.atlas_region
}

resource "mongodbatlas_network_peering" "aws" {
  project_id             = var.atlas_project_id
  container_id           = local.container_id
  provider_name          = "AWS"
  accepter_region_name   = var.aws_region
  aws_account_id         = data.aws_caller_identity.current.account_id
  vpc_id                 = aws_vpc.main.id
  route_table_cidr_block = var.vpc_cidr
}

resource "aws_vpc_peering_connection_accepter" "atlas" {
  vpc_peering_connection_id = mongodbatlas_network_peering.aws.connection_id
  auto_accept               = true
  tags                      = { Name = "${local.name}-atlas-peer" }
}

resource "aws_route" "to_atlas" {
  route_table_id            = aws_route_table.main.id
  destination_cidr_block    = local.atlas_cidr_eff
  vpc_peering_connection_id = aws_vpc_peering_connection_accepter.atlas.vpc_peering_connection_id
}

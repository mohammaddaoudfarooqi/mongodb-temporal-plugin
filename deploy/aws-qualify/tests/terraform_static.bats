#!/usr/bin/env bats
# Static checks on the qualification Terraform.

TF="$BATS_TEST_DIRNAME/../terraform"

@test "only the allowed AWS resource types are declared" {
  allowed='^(aws_vpc|aws_subnet|aws_internet_gateway|aws_route_table|aws_route_table_association|aws_route|aws_security_group|aws_key_pair|aws_instance|aws_vpc_peering_connection_accepter)$'
  types=$(grep -ho 'resource "aws_[a-z_]*"' "$TF"/*.tf | cut -d'"' -f2 | sort -u)
  [ -n "$types" ]
  for t in $types; do
    [[ "$t" =~ $allowed ]] || { echo "disallowed AWS resource type: $t"; return 1; }
  done
}

@test "no IAM, SSM or S3 usage anywhere" {
  ! grep -nE 'aws_iam_|aws_ssm_|aws_s3_|iam_instance_profile' "$TF"/*.tf
}

@test "no Atlas project is created or destroyed" {
  ! grep -n 'resource "mongodbatlas_project"' "$TF"/*.tf
}

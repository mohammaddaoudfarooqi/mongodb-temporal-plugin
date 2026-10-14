# SSH from the operator's /32 and allowed ranges, and traffic between the hosts (Temporal
# membership and gRPC); nothing else inbound.
resource "aws_security_group" "hosts" {
  name        = "${local.name}-sg"
  description = "qualification hosts: SSH from the operator, all traffic between hosts"
  vpc_id      = aws_vpc.main.id
  tags        = { Name = "${local.name}-sg" }

  ingress {
    description = "SSH from the operator and allowed ranges"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.admin_cidrs
  }

  ingress {
    description = "Between qualification hosts"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    description = "Atlas over the peering, package mirrors, Atlas SRV DNS"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

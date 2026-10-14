data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-kernel-6.1-x86_64"]
  }
  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

resource "aws_key_pair" "qualify" {
  key_name   = "${local.name}-key"
  public_key = file(pathexpand(var.public_key_path))
}

locals {
  ami_id = var.ami_id != "" ? var.ami_id : data.aws_ami.al2023.id
  # No secrets in user data: the URI arrives later over SSH.
  user_data = templatefile("${path.module}/../remote/bootstrap.sh.tftpl", {
    shutdown_minutes = local.max_hours * 60
  })
}

resource "aws_instance" "server" {
  count                                = local.server_count
  ami                                  = local.ami_id
  instance_type                        = local.server_type
  subnet_id                            = aws_subnet.main.id
  vpc_security_group_ids               = [aws_security_group.hosts.id]
  associate_public_ip_address          = true
  key_name                             = aws_key_pair.qualify.key_name
  user_data                            = local.user_data
  instance_initiated_shutdown_behavior = local.shutdown_behavior

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = 30
    encrypted   = true
  }

  depends_on = [aws_route_table_association.main]
  tags       = { Name = "${local.name}-server-${count.index + 1}", Role = "server" }
}

resource "aws_instance" "loadgen" {
  count                                = local.loadgen_count
  ami                                  = local.ami_id
  instance_type                        = local.loadgen_type
  subnet_id                            = aws_subnet.main.id
  vpc_security_group_ids               = [aws_security_group.hosts.id]
  associate_public_ip_address          = true
  key_name                             = aws_key_pair.qualify.key_name
  user_data                            = local.user_data
  instance_initiated_shutdown_behavior = local.shutdown_behavior

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = 20
    encrypted   = true
  }

  depends_on = [aws_route_table_association.main]
  tags       = { Name = "${local.name}-loadgen", Role = "loadgen" }
}

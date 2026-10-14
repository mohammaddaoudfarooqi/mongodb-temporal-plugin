#!/usr/bin/env bats
# Mock-parity contract for the terraform stub: the state-list behaviour
# qualify.sh relies on, checked against REAL terraform on an empty
# module (no providers, no network).

setup() {
  command -v terraform >/dev/null || skip "terraform not installed"
  M="$BATS_TEST_TMPDIR/m"; mkdir -p "$M"; : > "$M/main.tf"
  terraform -chdir="$M" init -input=false >/dev/null
}

@test "parity: no state file -> 'No state file was found!' and a non-zero exit" {
  run terraform -chdir="$M" state list
  [ "$status" -ne 0 ]
  [[ "$output" == *"No state file was found!"* ]]
}

@test "parity: an empty state lists nothing and exits zero" {
  echo '{"version":4,"terraform_version":"1.13.0","serial":1,"lineage":"x","outputs":{},"resources":[]}' > "$M/terraform.tfstate"
  run terraform -chdir="$M" state list
  [ "$status" -eq 0 ]
  [ -z "$output" ]
}

@test "parity: a corrupt state exits non-zero" {
  echo garbage > "$M/terraform.tfstate"
  run terraform -chdir="$M" state list
  [ "$status" -ne 0 ]
}

# Workspaces: TF_WORKSPACE selects the state per process,
# so two parallel runs never share .terraform/environment.
@test "parity: TF_WORKSPACE keeps each profile's state under terraform.tfstate.d/<profile>/" {
  echo 'resource "terraform_data" "x" {}' > "$M/main.tf"
  terraform -chdir="$M" workspace new functional >/dev/null
  terraform -chdir="$M" workspace new load >/dev/null
  terraform -chdir="$M" workspace select default >/dev/null
  TF_WORKSPACE=functional run terraform -chdir="$M" apply -auto-approve -input=false
  [ "$status" -eq 0 ]
  [ -f "$M/terraform.tfstate.d/functional/terraform.tfstate" ]
  [ ! -e "$M/terraform.tfstate.d/load/terraform.tfstate" ]
  TF_WORKSPACE=load run terraform -chdir="$M" state list
  [ "$status" -ne 0 ]
  [[ "$output" == *"No state file was found!"* ]]
  run cat "$M/.terraform/environment"
  [[ "$output" != *functional* && "$output" != *load* ]]
}

# AWS + Atlas release qualification

Spins up a VPC-peered Atlas cluster and EC2 hosts in us-east-1. It runs
the plugin's live tests against a git ref, copies the results to your
machine, then destroys everything and checks that nothing is left.

| Profile | Question it answers | Infrastructure | Time | Cost |
|---|---|---|---|---|
| `functional` | Does it work at all? | Atlas M10; 1 × c6i.large | ~1 h | < $0.50 |
| `load` | What does a production-shaped deployment sustain? | Atlas M30 (or `--atlas-tier`); 2 × c6i.2xlarge servers (`--servers`); 1 × c6i.xlarge load generator | ~4.5 h with the 2 h soak | ~$6–7 on M30 |

## What you need

- **Tools:** `terraform` ≥ 1.13, `aws` CLI, `jq`, `go`, `git`, `python3`,
  `ssh`/`scp` and `curl`. `bats` and `shellcheck` are needed only for
  `make qualify-test` and `make qualify-lint`.
- **An Atlas project for this harness**, and a **project-level API
  key with Project Owner** on it. The harness never creates or deletes
  projects. It only creates and deletes the cluster, peering, access
  list entry and database user inside the project. If the project
  already has an AWS network container in us-east-1, it is reused; its
  CIDR must not overlap `10.40.0.0/16`.
- **AWS credentials** that can create and delete VPCs, subnets,
  internet gateways, route tables and routes, security groups, key
  pairs, EC2 instances and VPC peering accepters, plus `ec2:Describe*`
  and `sts:GetCallerIdentity`. No IAM, SSM or S3 is used.

## Run it

```bash
cd deploy/aws-qualify
cp .env.example .env          # already created for you; fill in the values
scripts/qualify.sh all --profile functional --ref main
scripts/qualify.sh all --profile load --ref v1.2.0 --atlas-tier M50 --soak 2h
```

### Both profiles at once, with short-lived AWS credentials

Each profile has its own Terraform workspace, `.run/<profile>/`
directory and VPC range (functional `10.40.0.0/16`, load
`10.41.0.0/16`), so the two can run side by side. AWS credentials
are needed only while Terraform runs. Deploying, running and
collecting use SSH and the Atlas key. So with temporary AWS
credentials that expire in minutes:

```bash
scripts/qualify.sh up --profile functional --ref <ref> &
scripts/qualify.sh up --profile load --ref <ref> &
wait     # AWS work is done in the first ~3 minutes
for p in functional load; do
  (scripts/qualify.sh deploy --profile $p && scripts/qualify.sh run --profile $p;
   scripts/qualify.sh collect --profile $p) &
done
wait
# refresh the AWS_* values in .env, then:
scripts/qualify.sh down --profile functional --yes
scripts/qualify.sh down --profile load --yes
```

The project's Atlas network container is shared by every run. The
harness creates it when it is missing, never lets a stack own it,
and deletes it after the last run's teardown. It is never deleted if
it existed before the harness created anything.

`all` runs `up` → `deploy` → `run` → `collect`, then always `down`,
even after a failure. Pass `--keep` to leave the stack up for
inspection, then run `scripts/qualify.sh down` when you're done. Each
step can also be run on its own. Only one run can exist at a time.

`up` waits for the Atlas cluster, which takes 10–20 minutes.

## What runs

**functional**
1. All 12 e2e scenarios with aggregation visibility.
2. All 12 e2e scenarios with Atlas Search.
3. The Atlas live gates: Atlas Search lifecycle, the schema CLI's
   Atlas Search commands, and the Atlas conformance run.
4. An Atlas failover test triggered during a load.
5. A 500-workflow sanity load.

**load**
1. A smoke check.
2. A concurrency ladder that finds the highest concurrency where p99
   stays at or under 10 s and errors at or under 1%.
3. An Atlas failover, then the loss and return of a server node, both
   at 70% of that maximum.
4. A soak.

Server nodes find each other through the plugin's `cluster_membership`
table. Measurements wait until shard ownership has settled.

Test settings are in `profiles/<profile>.env`; infrastructure sizes are
in `profiles/<profile>.tfvars`.

## Results

`artifacts/<profile>-<run>/` contains:
- **`report.md`:** one **GO / NO-GO** verdict, then each phase, the
  ladder table, and host CPU and memory peaks. A host is flagged
  **host-bound** above 80% CPU, meaning the numbers may describe the
  host rather than the database.
- **Phase JSON files**, the host logs (with URI credentials masked)
  and the Atlas process metrics.

## Teardown checks

After `terraform destroy`, `down` checks three things:
- the Terraform state is empty;
- no EC2 instance, VPC, security group or key pair tagged with the
  run ID remains;
- the project has no cluster, peering or access list entry for the run.

If anything is left, it exits non-zero and lists it. Re-run `down`
once; if items persist, delete them in the console using the names it
printed (everything is named `tmc-<run>`).

Each host also terminates itself after `max_hours` (3 h functional,
8 h load), in case teardown never runs.

## Secrets

- `.env` and `.run/` are gitignored. `.env` is read as plain text and
  never executed.
- The Atlas key reaches `curl` only through a 0600 config file, and
  Terraform only through its environment.
- The connection string is a sensitive Terraform output. It reaches
  the hosts on SSH stdin and is never printed. The harness reads
  only named outputs, because a bare `terraform output -json` would
  print it.
- **The Terraform state** (`terraform/terraform.tfstate.d/`) holds the
  generated database passwords. Keep it private: it is gitignored; never
  sync or share it. Use an encrypted remote backend if you run the
  harness regularly. `down` deletes the database users.
- Use a dedicated Atlas project and short-lived AWS credentials. The
  first SSH connection to a new host accepts its key. See
  [the security guide](../../docs/operator/security.md#quickstart-and-qualification-scripts)
  for these and the residual risks.

## Developing the harness

```bash
make qualify-lint   # terraform fmt/validate, shellcheck, go vet
make qualify-test   # terraform test (mock providers), bats, go test
```

Both run offline. They prove the control flow and the secret handling.
Only a live run proves the cloud side.

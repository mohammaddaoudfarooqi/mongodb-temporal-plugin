# shellcheck shell=bash
# SSH/scp to qualification hosts, with a per-run
# known_hosts file and batch mode so nothing ever prompts.
# Bash 3.2 compatible (macOS /bin/bash): a plain array, no mapfile.
remote_init() {
  SSH_OPTS=(-i "$SSH_PRIVATE_KEY_PATH" -o BatchMode=yes -o ConnectTimeout=15
    -o ServerAliveInterval=30 -o ServerAliveCountMax=6
    -o StrictHostKeyChecking=accept-new -o "UserKnownHostsFile=$RUN/known_hosts" -o LogLevel=ERROR)
}

# rsh HOST CMD: run CMD on HOST as ec2-user; stdin is passed through.
rsh() {
  local host=$1; shift
  # shellcheck disable=SC2029 # the command is meant to expand here
  ssh "${SSH_OPTS[@]}" "ec2-user@$host" "$@"
}

# rcp SRC... DST: scp with the run's options; remote paths are host:path.
rcp() {
  scp -q -r "${SSH_OPTS[@]}" "$@"
}

# wait_host HOST: until SSH works and first boot has finished.
wait_host() {
  local host=$1 i
  for ((i = 0; i < 60; i++)); do
    if rsh "$host" 'grep -q "qualify bootstrap done" /var/log/qualify-boot.log' </dev/null 2>/dev/null; then
      return 0
    fi
    sleep "$POLL"
  done
  return 1
}

# shellcheck shell=bash
# Logging for qualify.sh: coloured on a TTY,
# every line also appended to $LOG. Lines pass through redact so a
# secret that reaches a message is masked.
if [[ -t 1 ]]; then C_G=$'\033[32m'; C_Y=$'\033[33m'; C_R=$'\033[31m'; C_B=$'\033[36m'; C_0=$'\033[0m'
else C_G=; C_Y=; C_R=; C_B=; C_0=; fi

# Values never printed; set by load_env.
SECRETS=()

redact() {
  local line
  while IFS= read -r line || [[ -n "$line" ]]; do
    for s in ${SECRETS[@]+"${SECRETS[@]}"}; do
      [[ -n "$s" ]] && line=${line//"$s"/<redacted>}
    done
    # Credentials embedded in any MongoDB URI.
    line=$(sed -E 's#(mongodb(\+srv)?://)[^@/ ]+@#\1<redacted>@#g' <<<"$line")
    printf '%s\n' "$line"
  done
}

_emit() { printf '%s\n' "$*" | redact | tee -a "${LOG:-/dev/null}"; }
log()  { _emit "${C_B}▸${C_0} $*"; }
ok()   { _emit "${C_G}✓${C_0} $*"; }
warn() { _emit "${C_Y}!${C_0} $*" >&2; }
die()  { _emit "${C_R}✗ $*${C_0}" >&2; exit 1; }

# run_logged CMD...: runs a command with its output redacted into the log.
run_logged() {
  "$@" 2>&1 | redact >> "${LOG:-/dev/null}"
  return "${PIPESTATUS[0]}"
}

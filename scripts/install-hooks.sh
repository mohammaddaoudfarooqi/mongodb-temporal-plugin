#!/bin/sh
# install-hooks.sh — copy the tracked pre-commit and commit-msg hooks into
# .git/hooks/ so local commits run the same checks as CI.

set -e
root=$(git rev-parse --show-toplevel)
hooks="$root/.git/hooks"

install_one() {
  src="$1"
  dst="$hooks/$(basename "$src")"
  cp "$src" "$dst"
  chmod +x "$dst"
  echo "installed: $dst"
}

install_one "$root/scripts/pre-commit"
install_one "$root/scripts/commit-msg"

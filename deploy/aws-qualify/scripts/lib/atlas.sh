# shellcheck shell=bash
# Atlas Admin API v2 over curl with digest auth. The key pair goes in a
# 0600 curl config file, never on the command line.
ATLAS_BASE=${ATLAS_BASE:-https://cloud.mongodb.com/api/atlas/v2}

atlas_init() {
  ATLAS_RC="$RUN/atlas.curlrc"
  (umask 077; printf 'user = "%s:%s"\ndigest\n' "$MONGODB_ATLAS_PUBLIC_KEY" "$MONGODB_ATLAS_PRIVATE_KEY" > "$ATLAS_RC")
}

# atlas_cleanup: remove the key file. Subshells create it too, so remove
# it by path, and never fail (this runs in the EXIT trap under set -e).
atlas_cleanup() { rm -f "$RUN/atlas.curlrc" "$RUN"/atlas.?????? 2>/dev/null || true; }

# atlas_api METHOD PATH VERSION OUTFILE: prints the HTTP status, writes
# the body to OUTFILE.
atlas_api() {
  local method=$1 path=$2 version=$3 out=$4
  curl -sS -K "$ATLAS_RC" -X "$method" --max-time 60 \
    -H "Accept: application/vnd.atlas.${version}+json" \
    -H "Content-Type: application/json" \
    -o "$out" -w '%{http_code}' "${ATLAS_BASE}${path}"
}

# atlas_get PATH: body on stdout; fails on non-2xx.
atlas_get() {
  local tmp code
  tmp=$(mktemp "$RUN/atlas.XXXXXX")
  code=$(atlas_api GET "$1" 2023-01-01 "$tmp") || { rm -f "$tmp"; return 1; }
  if [[ "$code" != 2* ]]; then rm -f "$tmp"; echo "Atlas GET $1 returned HTTP $code" >&2; return 1; fi
  cat "$tmp"; rm -f "$tmp"
}

atlas_project() { echo "/groups/$ATLAS_PROJECT_ID"; }

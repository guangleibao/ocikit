#!/bin/bash
# Collect the IMDSv2 namespace, rather than only /instance/.
# References:
# https://docs.oracle.com/en-us/iaas/Content/Compute/Tasks/gettingmetadata.htm
# https://github.com/oracle/oci-python-sdk/blob/master/src/oci/auth/signers/instance_principals_security_token_signer.py
set -euo pipefail

usage() {
    cat <<'HELP'
Usage: ./getInstanceMetadata.sh [--instance-only] [--endpoint PATH]

Collect IMDSv2 metadata as one JSON document on stdout. Diagnostics use stderr.
Probe /, /instance/, /vnics/, /volumeAttachments/, /host/, /identity/ and the
identity cert.pem, intermediate.pem and key.pem endpoints. Follow safe directory
listings to discover additional endpoints (up to 256 endpoints, depth 16).
JSON objects/arrays are kept in full, including custom/extended instance metadata;
text and PEM responses are preserved as strings. The report includes identity
private-key material and any user_data returned by IMDS.

  --endpoint PATH   Also probe an IMDS-relative path; repeatable, e.g. host/foo
  --instance-only   Original output: only the /instance/ JSON object
  -h, --help        Show help

Use OCI_METADATA_BASE_URL in the adjacent .env to override the default
http://169.254.169.254/opc/v2. No IMDSv1 fallback or redirects are used.
Every request includes Authorization: Bearer Oracle and bypasses proxies.
Each path is requested once. On any HTTP or transport error, record the failure
and immediately continue to the next path, without retries or backoff. Each
request has a 3-second connect timeout and 30-second total timeout.

Output: .metadata["/host/"] (and other successful paths), .endpoints (per-path
HTTP status, attempts and failures), and .summary. Missing optional paths (404 or
410) are recorded as unavailable. Exit 1 for other failures, missing/invalid
/instance/, or truncated discovery; successful partial results are still printed.
--instance-only cannot be combined with --endpoint.

Examples:
  ./getInstanceMetadata.sh | jq '.metadata["/host/"]'
  ./getInstanceMetadata.sh --endpoint host/some-key
  ./getInstanceMetadata.sh --instance-only | jq '.regionInfo'
HELP
}

# Parse before loading .env or checking dependencies so --help works anywhere.
instance_only=0
extra_paths=()
# Keep discovery inside this namespace: no URL references, traversal, query
# strings, fragments, percent escapes or option-like segments. A final slash is OK.
valid_path() {
    local segment remainder=${1%/}
    [[ -n "$remainder" && "$remainder" != /* && "$remainder" != */ ]] || return 1
    while :; do
        segment=${remainder%%/*}
        [[ "$segment" =~ ^[a-zA-Z0-9_~][a-zA-Z0-9_.~-]*$ ]] || return 1
        [[ "$remainder" == */* ]] || break
        remainder=${remainder#*/}
    done
}
while [ "$#" -gt 0 ]; do
    case "$1" in
        -h|--help) usage; exit 0 ;;
        --instance-only) instance_only=1; shift ;;
        --endpoint)
            if [ "$#" -lt 2 ] || ! valid_path "$2"; then
                echo 'Error: --endpoint requires an IMDS-relative path, e.g. host/.' >&2
                exit 2
            fi
            extra_paths+=("$2"); shift 2 ;;
        *) echo "Error: unknown argument: $1" >&2; exit 2 ;;
    esac
done
if [ "$instance_only" -eq 1 ] && [ "${#extra_paths[@]}" -gt 0 ]; then
    echo 'Error: --instance-only cannot be combined with --endpoint.' >&2
    exit 2
fi

OCIKIT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ -f "$OCIKIT_DIR/.env" ]; then
    set -a
    . "$OCIKIT_DIR/.env"
    set +a
fi
for dependency in curl jq; do
    if ! command -v "$dependency" >/dev/null 2>&1; then
        echo "Error: $dependency is required; run the preparation script for your OS." >&2
        exit 127
    fi
done
base_url=${OCI_METADATA_BASE_URL:-http://169.254.169.254/opc/v2}
base_url=${base_url%/}
case "$base_url" in
    http://*|https://*) ;;
    *) echo 'Error: OCI_METADATA_BASE_URL must be an HTTP(S) IMDSv2 base URL.' >&2; exit 2 ;;
esac

# Private temporary files preserve text (including trailing newlines) and avoid
# putting large metadata/user_data or PEM bodies in process arguments.
umask 077
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/ocikit-imds.XXXXXXXX")
trap 'rm -rf -- "$work_dir"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
: > "$work_dir/responses.jsonl"
paths=('instance/' 'vnics/' 'volumeAttachments/' 'host/' '' 'identity/'
       'identity/cert.pem' 'identity/intermediate.pem' 'identity/key.pem')
if [ "$instance_only" -eq 1 ]; then
    paths=('instance/')
elif [ "${#extra_paths[@]}" -gt 0 ]; then
    paths+=("${extra_paths[@]}")
fi
seen=()
index=0
truncated=false
while [ "$index" -lt "${#paths[@]}" ]; do
    path=${paths[$index]}
    index=$((index + 1))
    duplicate=0
    if [ "${#seen[@]}" -gt 0 ]; then
        for prior in "${seen[@]}"; do
            # /host and /host/ refer to the same discovery target.
            if [ "${prior%/}" = "${path%/}" ]; then duplicate=1; break; fi
        done
    fi
    [ "$duplicate" -eq 0 ] || continue
    if [ "${#seen[@]}" -ge 256 ]; then truncated=true; break; fi
    seen+=("$path")
    curl_exit=0
    : > "$work_dir/body"
    : > "$work_dir/error"
    # -q must be first: ignore .curlrc, which could add retries or redirects.
    # Make exactly one attempt, including for 404/429/5xx and network errors.
    http_status=$(curl -q --noproxy '*' --silent --show-error \
        --connect-timeout 3 --max-time 30 \
        -H 'Authorization: Bearer Oracle' \
        --output "$work_dir/body" --write-out '%{http_code}' \
        "$base_url/$path" 2> "$work_dir/error") || curl_exit=$?
    state=error
    if [ "$curl_exit" -eq 0 ]; then
        case "$http_status" in
            2[0-9][0-9]) state=ok ;;
            404|410) state=unavailable ;;
        esac
    fi
    # Empty/non-JSON responses remain strings; false/null JSON values are valid.
    jq -cn --arg path "/$path" --arg status "$http_status" --arg state "$state" \
        --argjson curl_exit "$curl_exit" --argjson attempts 1 \
        --rawfile raw "$work_dir/body" --rawfile error "$work_dir/error" '
        (try {value: ($raw | fromjson), encoding: "json"}
         catch {value: $raw, encoding: "text"}) as $body |
        {path: $path, state: $state, http_status: ($status | tonumber? // 0),
         curl_exit_code: $curl_exit, attempts: $attempts, encoding: $body.encoding,
         body: $body.value, error: $error}' >> "$work_dir/responses.jsonl"
    if [ "$state" != ok ]; then
        printf '/%s: %s (HTTP %s; curl exit %s).\n' "$path" "$state" "${http_status:-000}" "$curl_exit" >&2
        continue
    fi
    [ "$instance_only" -eq 0 ] || continue
    # Aggregate JSON already contains all nested metadata. Discover only actual
    # directory listings: JSON arrays of path strings or newline-separated names.
    # Keep the unmodified listing in the output as well as fetched child values.
    jq -Rrs '
        def segment: test("^[A-Za-z0-9_~][A-Za-z0-9_.~-]*/?$");
        . as $raw | (try fromjson catch null) as $json |
        (if ($json | type) == "array" then
             if all($json[]; type == "string") then $json else [] end
         elif $json == null and ($raw | contains("\n")) then
             $raw | split("\n") | map(rtrimstr("\r")) | map(select(length > 0))
         else [] end) |
        if length > 0 and all(.[]; segment) then .[] else empty end
        ' "$work_dir/body" > "$work_dir/children"
    parent=${path%/}
    while IFS= read -r child; do
        if [ -n "$parent" ]; then child="$parent/$child"; fi
        valid_path "$child" || continue
        slashes=${child//[^\/]}
        if [ "${#slashes}" -gt 16 ]; then truncated=true; continue; fi
        # Bound the queue, not just the number of HTTP requests.
        if [ "${#paths[@]}" -ge 512 ]; then truncated=true; break; fi
        paths+=("$child")
    done < "$work_dir/children"
done

jq -s --arg base_url "$base_url" --argjson truncated "$truncated" '
    . as $responses |
    {base_url: $base_url, collected_at: (now | todateiso8601),
     metadata: (map(select(.state == "ok") | {key: .path, value: .body}) | from_entries),
     endpoints: (map({key: .path, value: (del(.path) |
         if .state == "ok" then del(.body, .error) else . end)}) | from_entries),
     summary: {successful: (map(select(.state == "ok")) | length),
               unavailable: (map(select(.state == "unavailable")) | length),
               errors: (map(select(.state == "error")) | length),
               discovery_truncated: $truncated,
               instance_valid: any($responses[]; .path == "/instance/" and
                   .state == "ok" and .encoding == "json" and (.body | type) == "object")}}
    ' "$work_dir/responses.jsonl" > "$work_dir/report.json"
if [ "$instance_only" -eq 1 ]; then
    if ! jq -e '.summary.instance_valid' "$work_dir/report.json" >/dev/null; then
        echo 'Error: /instance/ did not return an instance metadata JSON object.' >&2
        exit 1
    fi
    jq '.metadata["/instance/"]' "$work_dir/report.json"
else
    cat "$work_dir/report.json"
fi
jq -e '.summary | .errors == 0 and .instance_valid and (.discovery_truncated | not)' \
    "$work_dir/report.json" >/dev/null

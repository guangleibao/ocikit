#!/bin/bash
# Download OCI Object Storage objects using the instance's identity.
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage: ./download.sh [OPTIONS] SOURCE [DESTINATION]

Download one object, or a directory with --recursive. Requires OCI CLI on an
OCI instance whose dynamic group can read objects (and list them for recursion).

Options:
  -b, --bucket NAME    Bucket (default: OCI_OS_BUCKET in .env, otherwise stage)
  -r, --recursive      Download a directory and its subdirectories
      --overwrite      Replace existing local files (default: skip them)
  -h, --help           Show this help

Loads .env beside this script: OCI_OS_NAMESPACE is required; OCI_REGION and
OCI_OS_BUCKET are optional. CLI options override the bucket default.
Authentication is always instance_principal, regardless of OCI_AUTH.

SOURCE is an exact object name for a single file. With --recursive it is a
directory prefix; a trailing / is added automatically. Use an empty SOURCE
(quoted as '') to download the entire bucket.

DESTINATION defaults to the current directory. For a single object, an existing
directory receives the object's basename; otherwise DESTINATION is a filename.
Recursive downloads preserve full object paths under DESTINATION, including
the selected prefix (reports/a.csv becomes DESTINATION/reports/a.csv).

Examples:
  ./download.sh --bucket my-bucket reports/a.csv ./
  ./download.sh --bucket my-bucket reports/a.csv ./renamed.csv
  ./download.sh --bucket my-bucket -r reports/ ./downloads
  ./download.sh --bucket my-bucket -r '' ./backup
USAGE
}

fail() { printf 'Error: %s\n' "$*" >&2; exit 2; }

# Allow help without credentials, dependencies or a .env file.
if [[ ${1:-} == --help || ${1:-} == -h ]]; then
    usage
    exit 0
fi

OCIKIT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [[ -f "$OCIKIT_DIR/.env" ]]; then
    set -a
    . "$OCIKIT_DIR/.env"
    set +a
fi

bucket=${OCI_OS_BUCKET:-stage}
recursive=false
overwrite=false
positional=()
while (( $# )); do
    case "$1" in
        -b|--bucket)
            (( $# >= 2 )) || fail "$1 requires a bucket name"
            bucket=$2
            shift 2
            ;;
        -r|--recursive) recursive=true; shift ;;
        --overwrite) overwrite=true; shift ;;
        -h|--help) usage; exit 0 ;;
        --) shift; positional+=("$@"); break ;;
        -*) fail "unknown option: $1 (see --help)" ;;
        *) positional+=("$1"); shift ;;
    esac
done

(( ${#positional[@]} >= 1 && ${#positional[@]} <= 2 )) ||
    fail 'provide SOURCE and optionally DESTINATION (see --help)'
[[ -n "$bucket" ]] || fail 'set OCI_OS_BUCKET in .env or use --bucket NAME'
[[ -n ${OCI_OS_NAMESPACE:-} ]] || fail 'set OCI_OS_NAMESPACE in .env beside this script'
command -v oci >/dev/null 2>&1 || fail 'OCI CLI is required; install oci and put it on PATH'

source_name=${positional[0]}
destination=${positional[1]:-.}
[[ -n "$destination" ]] || fail 'DESTINATION must not be empty'
common=(--auth instance_principal --namespace-name "$OCI_OS_NAMESPACE" --bucket-name "$bucket")
if [[ -n ${OCI_REGION:-} ]]; then
    common+=(--region "$OCI_REGION")
fi

if "$recursive"; then
    [[ ! -e "$destination" || -d "$destination" ]] || fail 'recursive DESTINATION must be a directory'
    args=(os object bulk-download "${common[@]}" --download-dir "$destination")
    if [[ -n "$source_name" ]]; then
        args+=(--prefix "${source_name%/}/")
    fi
    if "$overwrite"; then
        args+=(--overwrite)
    else
        args+=(--no-overwrite)
    fi
else
    [[ -n "$source_name" && "$source_name" != */ ]] || fail 'provide an object name, or use --recursive for directories'
    if [[ -d "$destination" || "$destination" == */ ]]; then
        mkdir -p -- "$destination"
        destination="${destination%/}/${source_name##*/}"
    fi
    # Prefix relative paths so a filename of '-' is not treated as CLI stdout.
    [[ "$destination" == /* ]] || destination="./$destination"
    if [[ -e "$destination" || -L "$destination" ]]; then
        [[ -f "$destination" && ! -L "$destination" ]] || fail "destination is not a regular file: $destination"
        if ! "$overwrite"; then
            printf 'Skipping existing file: %s\n' "$destination"
            exit 0
        fi
    fi
    mkdir -p -- "$(dirname -- "$destination")"
    args=(os object get "${common[@]}" --name "$source_name" --file "$destination")
fi

exec oci "${args[@]}"

#!/bin/bash

set -euo pipefail

# https://stackoverflow.com/a/246128
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

SCRIPTDIR="$BASEDIR/scripts"

INXML="$BASEDIR/examples/medline15n0572-s10.xml"
OUTDIR="$BASEDIR/example-output"

mkdir -p "$OUTDIR"

python3 "$SCRIPTDIR/converttees.py" "$INXML" -o "$OUTDIR"

echo "Done, output in $OUTDIR" >&2

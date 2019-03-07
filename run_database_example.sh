#!/bin/bash

set -euo pipefail

# https://stackoverflow.com/a/246128
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

SCRIPTDIR="$BASEDIR/scripts"

INXML="$BASEDIR/examples/medline15n0572-s10.xml"
OUTDB="$BASEDIR/example-output"

python3 "$SCRIPTDIR/converttees.py" "$INXML" -o "$OUTDB" -D

echo "Done, output in $OUTDB.sqlite" >&2
echo "(try lssqlite.py and catsqlite.py in scripts/ to see contents)" >&2

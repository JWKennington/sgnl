#!/usr/bin/env bash
OUTFILE="foo-$1.txt"
echo FOO > $OUTFILE
echo "Wrote file: $OUTFILE"
sleep 30

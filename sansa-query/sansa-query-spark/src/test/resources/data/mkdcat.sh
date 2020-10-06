#!/bin/bash
TMP=${1:-http://localhost/data/}
BASE_URL="$TMP" sparql-integrate --w=trig/pretty cwd=. mkdcat.sparql

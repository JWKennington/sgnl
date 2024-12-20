#!/usr/bin/env bash
###
### Author: rdt12@psu.edu
### Date:  Dec 20, 2024
### Desc: Create a SimpleJson data source for development environment.
### Usage: createSimpleJsonDataSource.sh DS_NAME DS_URL
###
if  which jq >/dev/null; then
    JQ="jq -r .message"
else
    "NOTE: jq not found, sending raw JSON output..."
    JQ="cat -"
fi

dsname=$1
dsurl=$2
echo "Attempting to create SimpleJson data source $dsname pointing at $dsurl"
(curl -su admin:sgnl -X POST  http://localhost/api/datasources  -H 'Accept: application/json'  -H  'Content-Type: application/json'  -d @-  <<EOF
{
  "name": "$dsname",
  "type": "grafana-simple-json-datasource",
  "access": "proxy",
  "url": "$dsurl",
  "user": "",
  "database": "",
  "basicAuth": false
}
EOF
) | $JQ
echo

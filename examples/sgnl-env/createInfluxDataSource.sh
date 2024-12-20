#!/usr/bin/env bash
###
### Author: rdt12@psu.edu
### Date:  Dec 20, 2024
### Desc: Create InfluxDB data source for development environment.
### Usage: createInfluxDataSource.sh DS_NAME DS_URL DS_DATABASE
###
if  which jq >/dev/null; then
    JQ="jq -r .message"
else
    "NOTE: jq not found, sending raw JSON output..."
    JQ="cat -"
fi

dsname=$1
dsurl=$2
dsdb=$3
echo "Attempting to create InfluxDB data source $dsname pointing at $dsurl with DB=$dsdb"
(curl -su admin:sgnl -X POST  http://localhost/api/datasources  -H 'Accept: application/json'  -H  'Content-Type: application/json'  -d @-  <<EOF
{
  "name": "$dsname",
  "type": "influxdb",
  "access": "proxy",
  "url": "$dsurl",
  "user": "",
  "database": "$dsdb",
  "basicAuth": false,
  "jsonData": { "httpMode": "POST"}
}
EOF
) | $JQ
echo

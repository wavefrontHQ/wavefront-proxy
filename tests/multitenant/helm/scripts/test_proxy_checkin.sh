#!/bin/bash

WF_URL=${1:-${WAVEFRONT_URL}}
WF_TOKEN=${2:-${WAVEFRONT_TOKEN}}
ID=${PROXY_ID:=$(cat "/tmp/id")}

for i in 1 2 3 4 5
do
    echo "Checkin for Proxy '${ID}' (test:$i)"
    test=$(curl \
            --silent -X 'GET' \
            "${WF_URL}v2/proxy/${ID}" \
            -H 'accept: application/json' \
            -H "Authorization: Bearer ${WF_TOKEN}")
    
    status=$(echo ${test} | sed -n 's/.*"status":"\([^"]*\)".*/\1/p')
    if [ "${status}" = "ACTIVE" ] 
    then
        exit 0
    fi
    
    echo "Proxy not found, sleep 15 secs and try again"
    sleep 15
done
exit -1

#!/bin/bash

PROXY_ID=$(cat "/config/id")

echo "Checkin for Proxy '${PROXY_ID}'"

echo =========
env
echo =========

curl \
    -o /dev/null -w '%{http_code}' \
    -X 'GET' \
    "https://nimba.wavefront.com/api/v2/proxy/${PROXY_ID}" \
    -H 'accept: application/json' \
    -H 'X-WF-CSRF-TOKEN: vMVcd04A-07_0il99gPHv-Hyb-my7ZnmxP__dJaebzeowNHsVr9LQA4LTTJAgDn3dN12huLTzdlwlInKiQ' 

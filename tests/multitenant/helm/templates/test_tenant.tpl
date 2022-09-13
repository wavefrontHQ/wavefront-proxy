{{- define "test_tenant" }}
#!/bin/bash

URL=${WF_URL:-'{{ .Values.wavefront_2.url }}'}
TOKEN=${WF_TOKEN:-'{{ .Values.wavefront_2.token }}'}
ID=${PROXY_ID:=$(cat "/tmp/id")}

sleep 15

for i in 1 2 3 4 5
do
    echo "Checkin for Proxy '${ID}' (test:$i)"
    curl \
        -f -X 'GET' \
        "${URL}v2/proxy/${ID}" \
        -H 'accept: application/json' \
        -H "Authorization: Bearer ${TOKEN}" 
    
    if [ $? -eq 0 ] 
    then
        exit 0
    fi
    
    echo "Proxy not found, sleep 15 secs and try again"
    sleep 15
done
exit -1
{{- end }}

# {{- define "proxy-checking-test" }}
#!/bin/bash

URL=${WF_URL:-'{{ .Values.wavefront.url }}'}
TOKEN=${WF_TOKEN:-'{{ .Values.wavefront.token }}'}
ID=${PROXY_ID:=$(cat "/config/id")}

sleep 15

for i in 1 2 3 4 5
do
    echo "Checkin for Proxy '${ID}' (test:$i)"
    curl \
        -f -i -o - --silent -X 'GET' \
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
# {{- end }}

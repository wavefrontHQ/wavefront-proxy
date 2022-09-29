#!/bin/bash -xe

wait_proxy_up(){
    echo "Waiting proxy to open on 2878..."
    while ! bash -c "echo > /dev/tcp/localhost/2878"; do   
    sleep 1
    done
    echo "done"
}

get_push_count(){
    test=$(curl \
            --silent -X 'GET' \
            "${WAVEFRONT_URL}v2/chart/raw?source=$(hostname)&metric=~proxy.push.${1}.http.200.count" \
            -H 'accept: application/json' \
            -H "Authorization: Bearer ${WAVEFRONT_TOKEN}")
    points=$(echo $test | jq 'map(.points) | flatten | sort_by(.timestamp)[-1].value')
    echo ${points}
}

wait_push_count_not_zero(){
    while true
    do
        v=$(get_push_count $1)
        echo "${v}"
        if [ "${v}" -ne 0 ]
        then
            return
        fi
        sleep 15
    done
}

generate_load(){
    for i in {0..10}
    do
        curl "http://localhost:2878/logs/json_array?f=logs_json_arr" \
            --silent -X POST \
            -d "[{\"message\":\"INFO  local log line 1\",\"from_proxy\":\"true\",\"source\":\"$(hostname)\",\"timestamp\":\"$(date +%s)000\"}]"
        sleep 1
    done
}

wait_proxy_up

generate_load

sleep 60

wait_push_count_not_zero logs

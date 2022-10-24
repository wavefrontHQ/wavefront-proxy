#!/bin/bash -xe

wait_proxy_up(){
    echo "Waiting proxy to open on 2878..."
    while ! bash -c "echo > /dev/tcp/localhost/2878"; do   
    sleep 1
    done
    echo "done"
}

truncate_buffer(){
    curl \
        --silent -X 'PUT' \
        -H 'Content-Type: application/json' \
        -H "Authorization: Bearer ${WAVEFRONT_TOKEN}" \
        "${WAVEFRONT_URL}v2/proxy/${ID}" \
        -d '{"shutdown":false ,"truncate":true}'
}

shutdown_proxy(){
    curl \
        --silent -X 'PUT' \
        -H 'Content-Type: application/json' \
        -H "Authorization: Bearer ${WAVEFRONT_TOKEN}" \
        "${WAVEFRONT_URL}v2/proxy/${ID}" \
        -d '{"shutdown":true ,"truncate":false}'
}

get_buffer_points(){
    test=$(curl \
            --silent -X 'GET' \
            "${WAVEFRONT_URL}v2/chart/raw?source=disk-buffer-test-proxy&metric=~proxy.buffer.${1}.points.points" \
            -H 'accept: application/json' \
            -H "Authorization: Bearer ${WAVEFRONT_TOKEN}")
    points=$(echo $test | jq 'map(.points) | flatten | sort_by(.timestamp)[-1].value')
    echo ${points}
}

wait_buffer_have_points(){
    while true
    do
        sleep 15
        v=$(get_buffer_points $1)
        echo "${v}"
        if [ "${v}" -eq "${2}" ]
        then
            return
        fi
    done
}

send_metrics(){
    METRICNAME_A="test.gh.buffer-disk.${RANDOM}${RANDOM}"
    for i in {0..99}
    do
        curl http://localhost:2878 -X POST -d "${METRICNAME_A} ${RANDOM} source=github_proxy_action"
    done
}

/bin/bash /opt/wavefront/wavefront-proxy/run.sh & 
wait_proxy_up
ID=${PROXY_ID:=$(cat "/tmp/id")}

wait_buffer_have_points memory 0
send_metrics
wait_buffer_have_points memory 100
shutdown_proxy

sleep 120

/bin/bash /opt/wavefront/wavefront-proxy/run.sh & 
wait_buffer_have_points memory 0
wait_buffer_have_points disk 100
truncate_buffer
wait_buffer_have_points disk 0

shutdown_proxy
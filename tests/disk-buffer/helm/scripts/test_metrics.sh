#!/bin/bash -xe
echo "Waiting proxy to open on 2878..."

while ! bash -c "echo > /dev/tcp/localhost/2878"; do   
  sleep 1
done

echo "done"

ID=${PROXY_ID:=$(cat "/tmp/id")}

truncate_buffer(){
    curl \
        -X 'PUT' \
        -H 'Content-Type: application/json' \
        -H "Authorization: Bearer ${WAVEFRONT_TOKEN}" \
        "${WAVEFRONT_URL}v2/proxy/${ID}" \
        -d '{"shutdown":false ,"truncate":true}'

    wait_buffer_have_points 0
}

get_buffer_points(){
    test=$(curl \
            --silent -X 'GET' \
            "${WAVEFRONT_URL}v2/chart/raw?source=disk-buffer-test-proxy&metric=~proxy.buffer.disk.points.points" \
            -H 'accept: application/json' \
            -H "Authorization: Bearer ${WAVEFRONT_TOKEN}")
    points=$(echo $test | jq -c '.[0].points | sort_by(.timestamp)[-1].value')
    echo ${points}
}

wait_buffer_have_points(){
    DONE=false
    while [ !"${DONE}" ]
    do
        sleep 15
        v=$(get_buffer_points)
        echo "${v}"
        if [ "${v}" -eq "${1}" ]
        then
            DONE=true
        fi
    done
}

truncate_buffer

METRICNAME_A="test.gh.buffer-disk.${RANDOM}${RANDOM}"
curl http://localhost:2878 -X POST -d "${METRICNAME_A} ${RANDOM} source=github_proxy_action"
curl http://localhost:2878 -X POST -d "${METRICNAME_A} ${RANDOM} source=github_proxy_action"
curl http://localhost:2878 -X POST -d "${METRICNAME_A} ${RANDOM} source=github_proxy_action"
curl http://localhost:2878 -X POST -d "${METRICNAME_A} ${RANDOM} source=github_proxy_action"
curl http://localhost:2878 -X POST -d "${METRICNAME_A} ${RANDOM} source=github_proxy_action"
curl http://localhost:2878 -X POST -d "${METRICNAME_A} ${RANDOM} source=github_proxy_action"
curl http://localhost:2878 -X POST -d "${METRICNAME_A} ${RANDOM} source=github_proxy_action"
curl http://localhost:2878 -X POST -d "${METRICNAME_A} ${RANDOM} source=github_proxy_action"
curl http://localhost:2878 -X POST -d "${METRICNAME_A} ${RANDOM} source=github_proxy_action"
curl http://localhost:2878 -X POST -d "${METRICNAME_A} ${RANDOM} source=github_proxy_action"

wait_buffer_have_points 10

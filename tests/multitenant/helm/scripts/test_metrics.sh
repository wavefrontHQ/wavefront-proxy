#!/bin/bash
echo "Waiting proxy to open on 2878..."

while ! bash -c "echo > /dev/tcp/localhost/2878"; do   
  sleep 1
done

echo "done"

check_metric () {
    for i in 1 2 3 4 5
    do
        test=$(curl \
                    --silent -X 'GET' \
                    "${1}v2/chart/metric/detail?m=${3}" \
                    -H 'accept: application/json' \
                    -H "Authorization: Bearer ${2}")

        status=$(echo ${test} | sed -n 's/.*"last_update":\([^"]*\).*/\1/p')
        if [ ! -z "${status}" ] 
        then
            echo "metric '${3}' found."
            return 0
        fi
        echo "metric '${3}' not found, sleeping 10 secs and try again."
        sleep 10
    done
    return 1
}

ckeck_OK(){
    if [ $1 -ne $2 ] 
    then
        echo "KO"
        exit -1
    fi
}

# this should go to the main WFServer
METRICNAME_A="test.gh.multitenat.main.${RANDOM}${RANDOM}"
# this should go to the main WFServer and the tenant1 WFServer
METRICNAME_B="${METRICNAME_A}_bis"

curl http://localhost:2878 -X POST -d "${METRICNAME_A} ${RANDOM} source=github_proxy_action"
curl http://localhost:2878 -X POST -d "${METRICNAME_B} ${RANDOM} source=github_proxy_action multicastingTenantName=tenant1"

check_metric "${WAVEFRONT_URL}" "${WAVEFRONT_TOKEN}" "${METRICNAME_A}"
ckeck_OK $? 0 #found

check_metric "${WAVEFRONT_URL}" "${WAVEFRONT_TOKEN}" "${METRICNAME_B}"
ckeck_OK $? 0 #found

check_metric "${WAVEFRONT_URL_2}" "${WAVEFRONT_TOKEN_2}" "${METRICNAME_A}"
ckeck_OK $? 1 #not found

check_metric "${WAVEFRONT_URL_2}" "${WAVEFRONT_TOKEN_2}" "${METRICNAME_B}"
ckeck_OK $? 0 #found

echo "OK"

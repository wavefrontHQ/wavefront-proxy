#/bin/bash

cd /opt/loadgen && \
    java \
    -Dlog4j.configurationFile=./log4j2.xml \
    -jar loadgen.jar \
    --loadgenConfigPath ./config/loadgen_config.yaml \
    --pps 10000 \
    --useSingleClient false
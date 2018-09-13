#!/bin/bash
set -x

spool_dir="/var/spool/wavefront-proxy"
mkdir -p $spool_dir

java_heap_usage=${JAVA_HEAP_USAGE:-4G}
java \
	-Xmx$java_heap_usage -Xms$java_heap_usage $JAVA_ARGS\
	-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager \
	-Dlog4j.configurationFile=/etc/wavefront/wavefront-proxy/log4j2.xml \
	-jar /opt/wavefront/wavefront-proxy/bin/wavefront-push-agent.jar \
	-h $WAVEFRONT_URL \
	-t $WAVEFRONT_TOKEN \
	--hostname ${WAVEFRONT_HOSTNAME:-$(hostname)} \
	--ephemeral true \
	--buffer ${spool_dir}/buffer} \
	--flushThreads 6 \
	--retryThreads 6 \
	$WAVEFRONT_PROXY_ARGS

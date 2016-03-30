#!/bin/bash -e
USER=wavefront
GROUP=wavefront
WAVEFRONT_DIR=/opt/$USER
PROXY_DIR=$WAVEFRONT_DIR/wavefront-proxy
export JAVA_HOME=$PROXY_DIR/jre

# Set up wavefront user.
if ! groupmod $GROUP &> /dev/null; then
	groupadd $GROUP &> /dev/null
fi
if ! id $USER &> /dev/null; then
	useradd -r -s /bin/bash -g $GROUP $USER &> /dev/null
fi
chown -R $USER:$GROUP /opt/wavefront/wavefront-proxy

# Configure agent to start on reboot.
if [[ -f /etc/debian_version ]]; then
	update-rc.d wavefront-proxy defaults 99
elif [[ -f /etc/redhat-release ]] || [[ -f /etc/system-release-cpe ]]; then
	chkconfig --level 345 wavefront-proxy on
fi

# If there is an errant pre-3.8 agent running, we need to kill it.
if [[ -f /var/run/wavefront.pid ]]; then
	PID=`cat /var/run/wavefront.pid`
	kill -9 $PID || true
fi

service wavefront-proxy restart

exit 0

#!/bin/bash -e

# Remove startup entries for wavefront-proxy.
if [[ -f /etc/debian_version ]]; then
	update-rc.d -f wavefront-proxy remove
elif [[ -f /etc/redhat-release ]] || [[ -f /etc/system-release-cpe ]]; then
	chkconfig --del wavefront-proxy
fi

# Wavefront-proxy may not have been stopped cleanly before this package
# was removed. This is usually a problem since the user either doesn't
# want the proxy on their system at all, or they're about to install
# a newer version which could conflict with the currently running version.

if [[ -f /var/run/wavefront.pid ]]; then
	# Kill errant 3.8-or-older proxy.
	PID=`cat /var/run/wavefront.pid`
	kill -9 $PID || true
fi

if [[ -f /var/run/wavefront-proxy.pid ]]; then
	# Kill errant post-3.8 proxy.
	PID=`cat /var/run/wavefront-proxy.pid`
	kill -9 $PID || true
fi

exit 0

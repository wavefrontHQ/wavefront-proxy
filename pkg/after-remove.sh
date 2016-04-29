#!/bin/bash -e

# Remove startup entries for wavefront-proxy.
if [[ -f /etc/debian_version ]]; then
	update-rc.d -f wavefront-proxy remove
elif [[ -f /etc/redhat-release ]] || [[ -f /etc/system-release-cpe ]]; then
	chkconfig --del wavefront-proxy
fi

exit 0

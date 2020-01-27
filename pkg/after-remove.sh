#!/bin/bash -e

# Remove startup entries for wavefront-proxy if operation is un-install.
if [[ "$1" == "0" ]] || [[ "$1" == "remove" ]] || [[ "$1" == "purge" ]]; then
	if [[ -f /etc/debian_version ]]; then
		update-rc.d -f wavefront-proxy remove
	elif [[ -f /etc/redhat-release ]] || [[ -f /etc/system-release-cpe ]]; then
		chkconfig --del wavefront-proxy
	elif [[ -f /etc/SUSE-brand ]]; then
		systemctl disable wavefront-proxy
	fi
fi

exit 0

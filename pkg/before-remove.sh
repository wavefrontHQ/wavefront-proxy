#!/bin/bash -e

service wavefront-proxy stop || true

# If the proxy ID is in the default location, remove it.
if [[ -f /opt/wavefront/wavefront-proxy/.wavefront_id ]]; then
	rm /opt/wavefront/wavefront-proxy/.wavefront_id
fi

exit 0

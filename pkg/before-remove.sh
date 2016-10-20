#!/bin/bash -e

service_name="wavefront-proxy"
wavefront_dir="/opt/wavefront"
jre_dir="$wavefront_dir/$service_name/jre"

service wavefront-proxy stop || true

# If it's called with "0" argument (rpm) or "remove"/"purge" (deb), remove JRE as well. Otherwise keep it.
if [[ "$1" == "0" ]] || [[ "$1" == "remove" ]] || [[ "$1" == "purge" ]]; then
    echo "Removing installed JRE from $jre_dir"
    rm -rf $jre_dir
fi

exit 0

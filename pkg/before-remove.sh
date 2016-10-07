#!/bin/bash -e

service_name="wavefront-proxy"
wavefront_dir="/opt/wavefront"
jre_dir="$wavefront_dir/$service_name/jre"

service wavefront-proxy stop || true

rm -rf $jre_dir

exit 0

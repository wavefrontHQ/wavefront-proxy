#!/bin/bash -e

jre_dir="/opt/wavefront/wavefront-proxy/jre"

[[ -d $jre_dir ]] || mkdir -p $jre_dir

echo "Downloading and installing Zulu JRE"

curl -L --silent -o /tmp/jre.tar.gz https://s3-us-west-2.amazonaws.com/wavefront-misc/proxy-jre.tgz
tar -xf /tmp/jre.tar.gz --strip 1 -C $jre_dir
rm /tmp/jre.tar.gz

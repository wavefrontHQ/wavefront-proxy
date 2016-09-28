#!/bin/bash -e

jre_dir="/opt/wavefront/wavefront-proxy/jre"

[[ -d $jre_dir ]] || mkdir -p $jre_dir

echo "Downloading and installing Zulu 8.13.0.5"

curl -L --silent -o /tmp/jre.tar.gz http://cdn.azul.com/zulu/bin/zulu8.13.0.5-jdk8.0.72-linux_x64.tar.gz
tar -xf /tmp/jre.tar.gz --strip 1 -C $jre_dir
rm /tmp/jre.tar.gz

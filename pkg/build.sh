#!/bin/bash -ex

VERSION=$1
ITERATION=$2
cd $(dirname $0)

rm -rf build out

mkdir build
cp -r opt build/opt
cp -r etc build/etc
cp -r usr build/usr

mkdir -p build/opt/wavefront/wavefront-proxy/bin
mkdir -p build/usr/share/doc/wavefront-proxy/
mkdir -p build/opt/wavefront/wavefront-proxy

cp ../open_source_licenses.txt build/usr/share/doc/wavefront-proxy/
cp ../open_source_licenses.txt build/opt/wavefront/wavefront-proxy
cp wavefront-proxy.jar build/opt/wavefront/wavefront-proxy/bin

declare -A deps=(["deb"]="openjdk-11-jre" ["rpm"]="java-11-openjdk")

for target in deb rpm
do
	fpm \
		--after-install after-install.sh \
		--before-remove before-remove.sh \
		--after-remove after-remove.sh \
		--architecture all \
		--deb-no-default-config-files \
		--deb-priority optional \
		--depends ${deps[$target]} \
		--description "Proxy for sending data to Wavefront." \
		--exclude "*/.git" \
		--iteration $ITERATION \
		--license "Apache 2.0" \
		--log info \
		--maintainer "Wavefront" \
		--name wavefront-proxy \
		--rpm-os linux \
		--url https://www.wavefront.com \
		--vendor Wavefront \
		--version $VERSION \
		-C build \
		-s dir \
		-t ${target} \
		opt etc usr

	[[ -d out ]] || mkdir out
	mv *.${target} ../out
done 

rpm --delsign ../out/*.rpm

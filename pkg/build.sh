#!/bin/bash -e -x

if [[ $# -lt 5 ]]; then
	echo "Usage: $0 <jdk_zip_path> <proxy_jar_path> <fpm_target> <fpm_version> <fpm_iteration> [extra_packr_args...]"
	exit 1
fi

JDK=$1
JAR=$2
TARGET=$3
VERSION=$4
ITERATION=$5
shift 5

echo "Cleaning prior build run..."
rm -rf build
mkdir build

packr \
	--platform linux64 \
	--jdk "$JDK" \
	--executable wavefront-proxy \
	--appjar "$JAR" \
	--mainclass "com/wavefront/agent/PushAgent" \
	--vmargs "-Xmx4G" \
	--outdir "build/opt/wavefront/wavefront-proxy/bin" \
	$@

cp -R opt/* build/opt/
cp -R etc build/etc
cp -R usr build/usr

if [[ $TARGET == "deb" ]]; then
	EXTRA_DIRS="usr"
else
	EXTRA_DIRS=""
fi

fpm \
	--after-install postinst \
	--architecture amd64 \
	--config-files opt/wavefront/wavefront-proxy/conf/wavefront.conf \
	--deb-no-default-config-files \
	--deb-priority optional \
	--description "Proxy for sending data to Wavefront." \
	--iteration $ITERATION \
	--license "Apache 2.0" \
	--log info \
	--maintainer "Wavefront <support@wavefront.com>" \
	--name wavefront-proxy \
	--rpm-os linux \
	--url http://www.wavefront.com \
	--vendor Wavefront \
	--version $VERSION \
	-C build \
	-s dir \
	-t $TARGET \
	opt etc $EXTRA_DIRS

rpm --delsign *.rpm

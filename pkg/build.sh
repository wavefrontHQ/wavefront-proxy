#!/bin/bash -e -x

if [[ $# -lt 4 ]]; then
	echo "Usage: $0 <jdk_dir_path> <fpm_target> <fpm_version> <fpm_iteration> [extra_packr_args...]"
	exit 1
fi

JDK=$1
JDK=${JDK%/}

TARGET=$2
VERSION=$3
ITERATION=$4
shift 4

cd `dirname $0`
echo "Cleaning prior build run..."
rm -rf build

echo "Create build dirs..."
mkdir build
cp -R opt build/opt
cp -R etc build/etc
cp -R usr build/usr

BIN_DIR=build/opt/wavefront/wavefront-proxy/bin

echo "Make the agent jar..."
cd ..
mvn package -DskipTests=true
cd -
cp ../proxy/target/wavefront-push-agent.jar $BIN_DIR

echo "Stage the JDK..."
cp -r $JDK $BIN_DIR/jre

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

#!/bin/bash -e

function die {
	echo $@
	exit 1
}

PROG_DIR=`dirname $0`
cd $PROG_DIR
PROG_DIR=`pwd`
echo "Cleaning prior build run..."

cd ..
mvn clean
cd -

rm -rf build
if ls *.deb  &> /dev/null; then
	rm *.deb
fi
if ls *.rpm  &> /dev/null; then
	rm *.rpm
fi

if [[ $# -lt 4 ]]; then
	die "Usage: $0 <jdk_dir_path> <fpm_target> <fpm_version> <fpm_iteration>"
fi

JDK=$1
JDK=${JDK%/}
FPM_TARGET=$2
VERSION=$3
ITERATION=$4
shift 4

WF_DIR=`pwd`/build/opt/wavefront
PROXY_DIR=$WF_DIR/wavefront-proxy

echo "Create build dirs..."
mkdir build
cp -r opt build/opt
cp -r etc build/etc
cp -r usr build/usr

echo "Stage the JDK..."
cp -r $JDK $PROXY_DIR/jre

echo "Make jsvc..."
JSVC_BUILD_DIR="$PROXY_DIR/commons-daemon/src/native/unix"
cd $JSVC_BUILD_DIR
support/buildconf.sh
./configure --with-java=$PROXY_DIR/jre
make
cd $PROXY_DIR/bin
ln -s ../commons-daemon/src/native/unix/jsvc jsvc

echo "Make the agent jar..."
cd $PROG_DIR/..
mvn install -pl :java-lib -am
mvn package -pl :proxy -am
cd $PROG_DIR
cp ../proxy/target/wavefront-push-agent.jar $PROXY_DIR/bin

if [[ $FPM_TARGET == "deb" ]]; then
	EXTRA_DIRS="usr"
else
	EXTRA_DIRS=""
fi

fpm \
	--after-install after-install.sh \
	--after-remove after-remove.sh \
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
	-t $FPM_TARGET \
	opt etc $EXTRA_DIRS

if [[ $FPM_TARGET == "rpm" ]]; then
	rpm --delsign *.rpm
fi

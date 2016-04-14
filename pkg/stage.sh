#!/bin/bash -ex

function die {
	echo $@
	exit 1
}

PROG_DIR=`dirname $0`
cd $PROG_DIR
PROG_DIR=`pwd`
echo "Cleaning prior build run..."

rm -rf build
if ls *.deb  &> /dev/null; then
	rm *.deb
fi
if ls *.rpm  &> /dev/null; then
	rm *.rpm
fi

if [[ $# -lt 3 ]]; then
	die "Usage: $0 <jdk_dir_path> <commons_daemon_path> <push_agent_jar_path>"
fi

JDK=$1
JDK=${JDK%/}
COMMONS_DAEMON=$2
PUSH_AGENT_JAR=$3
shift 3

WF_DIR=`pwd`/build/opt/wavefront
PROXY_DIR=$WF_DIR/wavefront-proxy

echo "Create build dirs..."
mkdir build
cp -r opt build/opt
chmod 600 build/opt/wavefront/wavefront-proxy/conf/wavefront.conf
cp -r etc build/etc
cp -r usr build/usr

echo "Stage the JDK..."
cp -r $JDK $PROXY_DIR/jre

echo "Make jsvc..."
cp -r $COMMONS_DAEMON $PROXY_DIR
JSVC_BUILD_DIR="$PROXY_DIR/commons-daemon/src/native/unix"
cd $JSVC_BUILD_DIR
support/buildconf.sh
./configure --with-java=$PROXY_DIR/jre
make
cd $PROXY_DIR/bin
ln -s ../commons-daemon/src/native/unix/jsvc jsvc

echo "Make the agent jar..."
cd $PROG_DIR
[[ -f $PUSH_AGENT_JAR ]] || die "Bad agent jarfile given."
cp $PUSH_AGENT_JAR $PROXY_DIR/bin

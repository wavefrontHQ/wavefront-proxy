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
	die "Usage: $0 <jre_dir_path> <commons_daemon_path> <push_agent_jar_path>"
fi

git pull

JRE=$1
JRE=${JRE%/}
[[ -d $JRE ]] || die "Bad JRE given."

COMMONS_DAEMON=$2
[[ -d $COMMONS_DAEMON ]] || die "Bad agent commons-daemon given."

PUSH_AGENT_JAR=$3
[[ -f $PUSH_AGENT_JAR ]] || die "Bad agent jarfile given."

shift 3

WF_DIR=`pwd`/build/opt/wavefront
PROXY_DIR=$WF_DIR/wavefront-proxy

echo "Create build dirs..."
mkdir build
cp -r opt build/opt
cp -r etc build/etc
cp -r usr build/usr

COMMONS_DAEMON_COMMIT="COMMONS_DAEMON_1_2_2"
echo "Make jsvc at $COMMONS_DAEMON_COMMIT..."
cp -r $COMMONS_DAEMON $PROXY_DIR
cd $PROXY_DIR/commons-daemon
git pull
git reset --hard $COMMONS_DAEMON_COMMIT
JSVC_BUILD_DIR="$PROXY_DIR/commons-daemon/src/native/unix"
cd $JSVC_BUILD_DIR
support/buildconf.sh
./configure --with-java=$JRE
make
cd $PROXY_DIR/bin
ln -s ../commons-daemon/src/native/unix/jsvc jsvc

echo "Stage the agent jar..."
cd $PROG_DIR
cp $PUSH_AGENT_JAR $PROXY_DIR/bin/wavefront-proxy.jar

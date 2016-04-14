#!/bin/bash -ex

function die {
	echo $@
	exit 1
}

if [[ $# -lt 3 ]]; then
	die "Usage: $0 <fpm_target> <fpm_version> <fpm_iteration>"
fi

FPM_TARGET=$1
VERSION=$2
ITERATION=$3

if [[ $FPM_TARGET == "deb" ]]; then
	EXTRA_DIRS="usr"
else
	EXTRA_DIRS=""
fi

fpm \
	--after-install after-install.sh \
	--before-remove before-remove.sh \
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
	--url https://www.wavefront.com \
	--vendor Wavefront \
	--version $VERSION \
	-C build \
	-s dir \
	-t $FPM_TARGET \
	opt etc $EXTRA_DIRS

if [[ $FPM_TARGET == "rpm" ]]; then
	rpm --delsign *.rpm
fi

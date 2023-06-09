#!/bin/bash -x

DEB_FILE=$(find . -name "wavefront-proxy*deb")
JAR_FILE="/opt/wavefront/wavefront-proxy/bin/wavefront-proxy.jar"

if [ -f "${DEB_FILE}" ]; then
    echo "${DEB_FILE} exists."
else 
    echo "${DEB_FILE} does not exist."
    exit 100
fi

dpkg -i ${DEB_FILE}
retVal=$?
if [ ${retVal} -ne 0 ]; then
    echo "dpkg Error "${retVal}
    exit 101
fi

if [ -f "${JAR_FILE}" ]; then
    echo "${JAR_FILE} exists."
else 
    echo "${JAR_FILE} does not exist."
    exit 102
fi

exit 0

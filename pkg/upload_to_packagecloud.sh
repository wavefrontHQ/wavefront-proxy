#!/bin/bash -ex

if [[ $# -ne 3 ]]; then
	echo "Usage: $0 <packagecloud_repo> <config_file> <packages path>"
	exit 1
fi

ls -las ${3}

package_cloud push ${1}/any/any ${3}/*.deb --config=${2}
package_cloud push ${1}/rpm_any/rpm_any ${3}/*.rpm --config=${2}

wait

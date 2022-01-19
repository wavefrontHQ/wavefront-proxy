#!/bin/bash -ex

if [[ $# -ne 3 ]]; then
	echo "Usage: $0 <packagecloud_repo> <config_file> <packages path>"
	exit 1
fi

package_cloud push ${1}/el/7 ${3}/*.rpm --config=${2} &
package_cloud push ${1}/el/8 ${3}/*.rpm --config=${2} &
package_cloud push ${1}/el/6 ${3}/*.rpm --config=${2} &
package_cloud push ${1}/ol/8 ${3}/*.rpm --config=${2} &
package_cloud push ${1}/ol/7 ${3}/*.rpm --config=${2} &
package_cloud push ${1}/ol/6 ${3}/*.rpm --config=${2} &
package_cloud push ${1}/sles/12.0 ${3}/*.rpm --config=${2} &
package_cloud push ${1}/sles/12.1 ${3}/*.rpm --config=${2} &
package_cloud push ${1}/sles/12.2 ${3}/*.rpm --config=${2} &
package_cloud push ${1}/fedora/27 ${3}/*.rpm --config=${2} &
package_cloud push ${1}/opensuse/42.3 ${3}/*.rpm --config=${2} &
package_cloud push ${1}/debian/buster ${3}/*.deb --config=${2} &
package_cloud push ${1}/debian/stretch ${3}/*.deb --config=${2} &
package_cloud push ${1}/debian/wheezy ${3}/*.deb --config=${2} &
package_cloud push ${1}/debian/jessie ${3}/*.deb --config=${2} &
package_cloud push ${1}/ubuntu/focal ${3}/*.deb --config=${2} &
package_cloud push ${1}/ubuntu/eoan ${3}/*.deb --config=${2} &
package_cloud push ${1}/ubuntu/disco ${3}/*.deb --config=${2} &
package_cloud push ${1}/ubuntu/cosmic ${3}/*.deb --config=${2} &
package_cloud push ${1}/ubuntu/bionic ${3}/*.deb --config=${2} &
package_cloud push ${1}/ubuntu/artful ${3}/*.deb --config=${2} &
package_cloud push ${1}/ubuntu/zesty ${3}/*.deb --config=${2} &
package_cloud push ${1}/ubuntu/xenial ${3}/*.deb --config=${2} &
package_cloud push ${1}/ubuntu/trusty ${3}/*.deb --config=${2} &
package_cloud push ${1}/ubuntu/hirsute ${3}/*.deb --config=${2} &

wait

package_cloud push ${1}/any/any ${3}/*.deb --config=${2}
package_cloud push ${1}/rpm_any/rpm_any ${3}/*.rpm --config=${2}


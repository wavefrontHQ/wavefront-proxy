if [[ $# -ne 2 ]]; then
	echo "Usage: $0 <packagecloud_repo> <config_file>"
	exit 1
fi

package_cloud push --config=$2 $1/el/7 *.rpm &
package_cloud push --config=$2 $1/el/8 *.rpm &
package_cloud push --config=$2 $1/el/6 *.rpm &
package_cloud push --config=$2 $1/ol/8 *.rpm &
package_cloud push --config=$2 $1/ol/7 *.rpm &
package_cloud push --config=$2 $1/ol/6 *.rpm &
package_cloud push --config=$2 $1/sles/12.0 *.rpm &
package_cloud push --config=$2 $1/sles/12.1 *.rpm &
package_cloud push --config=$2 $1/sles/12.2 *.rpm &
package_cloud push --config=$2 $1/fedora/27 *.rpm &
package_cloud push --config=$2 $1/opensuse/42.3 *.rpm &
package_cloud push --config=$2 $1/debian/buster *.deb &
package_cloud push --config=$2 $1/debian/stretch *.deb &
package_cloud push --config=$2 $1/debian/wheezy *.deb &
package_cloud push --config=$2 $1/debian/jessie *.deb &
package_cloud push --config=$2 $1/ubuntu/focal *.deb &
package_cloud push --config=$2 $1/ubuntu/eoan *.deb &
package_cloud push --config=$2 $1/ubuntu/disco *.deb &
package_cloud push --config=$2 $1/ubuntu/cosmic *.deb &
package_cloud push --config=$2 $1/ubuntu/bionic *.deb &
package_cloud push --config=$2 $1/ubuntu/artful *.deb &
package_cloud push --config=$2 $1/ubuntu/zesty *.deb &
package_cloud push --config=$2 $1/ubuntu/xenial *.deb &
package_cloud push --config=$2 $1/ubuntu/trusty *.deb &

wait

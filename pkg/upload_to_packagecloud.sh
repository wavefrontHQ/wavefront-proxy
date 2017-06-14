if [[ $# -ne 2 ]]; then
	echo "Usage: $0 <packagecloud_repo> <config_file>"
	exit 1
fi

package_cloud push --config=$2 $1/el/7 *.rpm &
package_cloud push --config=$2 $1/el/6 *.rpm &
package_cloud push --config=$2 $1/debian/buster *.deb &
package_cloud push --config=$2 $1/debian/stretch *.deb &
package_cloud push --config=$2 $1/debian/wheezy *.deb &
package_cloud push --config=$2 $1/debian/jessie *.deb &
package_cloud push --config=$2 $1/ubuntu/xenial *.deb &
package_cloud push --config=$2 $1/ubuntu/trusty *.deb &
package_cloud push --config=$2 $1/ubuntu/zesty *.deb &

wait

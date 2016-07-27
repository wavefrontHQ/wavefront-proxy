if [[ $# -ne 2 ]]; then
	echo "Usage: $0 <packagecloud_repo> <config_file>"
	exit 1
fi

package_cloud --config=$2 push $1/el/7 *.rpm &
package_cloud --config=$2 push $1/el/6 *.rpm &
package_cloud --config=$2 push $1/debian/buster *.deb &
package_cloud --config=$2 push $1/debian/stretch *.deb &
package_cloud --config=$2 push $1/debian/wheezy *.deb &
package_cloud --config=$2 push $1/debian/jessie *.deb &
package_cloud --config=$2 push $1/ubuntu/xenial *.deb &
package_cloud --config=$2 push $1/ubuntu/precise *.deb &
package_cloud --config=$2 push $1/ubuntu/trusty *.deb &

wait

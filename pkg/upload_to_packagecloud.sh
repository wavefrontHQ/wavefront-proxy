if [[ $# -ne 3 ]]; then
	echo "Usage: $0 <packagecloud_repo> <config_file> <packages path>"
	exit 1
fi

package_cloud push --config=$2 $1/any_rpm/any_rpm $3/*.rpm &
package_cloud push --config=$2 $1/any/any $3/*.deb &

wait

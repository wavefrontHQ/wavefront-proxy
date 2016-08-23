#!/bin/bash -e
# These variables should match default values in /etc/init.d/wavefront-proxy
user="wavefront"
group="wavefront"
service_name="wavefront-proxy"
spool_dir="/var/spool/wavefront-proxy"
wavefront_dir="/opt/wavefront"

# Set up wavefront user.
if ! groupmod $group &> /dev/null; then
	groupadd $group &> /dev/null
fi
if ! id $user &> /dev/null; then
	useradd -r -s /bin/bash -g $group $user &> /dev/null
fi

# Create spool directory if it does not exist.
[[ -d $spool_dir ]] || mkdir -p $spool_dir && chown $user:$group $spool_dir

# Configure agent to start on reboot.
if [[ -f /etc/debian_version ]]; then
	update-rc.d $service_name defaults 99
elif [[ -f /etc/redhat-release ]] || [[ -f /etc/system-release-cpe ]]; then
	chkconfig --level 345 $service_name on
fi

# Allow system user to write .wavefront_id/buffer files to install dir.
chown $user:$group $wavefront_dir/$service_name

# If there is an errant pre-3.9 agent running, we need to kill it. This is
# required for a clean upgrade from pre-3.9 to 3.9+.
old_pid_file="/var/run/wavefront.pid"
if [[ -f $old_pid_file ]]; then
	pid=$(cat $old_pid_file)
	kill -9 "$pid" || true
	rm $old_pid_file
fi

service $service_name restart

exit 0

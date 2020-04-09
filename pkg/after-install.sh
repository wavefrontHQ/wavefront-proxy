#!/bin/bash -e
# These variables should match default values in /etc/init.d/wavefront-proxy
user="wavefront"
group="wavefront"
service_name="wavefront-proxy"
spool_dir="/var/spool/wavefront-proxy"
wavefront_dir="/opt/wavefront"
conf_dir="/etc/wavefront"
log_dir="/var/log/wavefront"
jre_dir="$wavefront_dir/$service_name/proxy-jre"

# Set up wavefront user.
if ! groupmod $group &> /dev/null; then
	groupadd $group &> /dev/null
fi
if ! id $user &> /dev/null; then
	useradd -r -s /bin/bash -g $group $user &> /dev/null
fi

# Create spool directory if it does not exist.
[[ -d $spool_dir ]] || mkdir -p $spool_dir && chown $user:$group $spool_dir

# Create log directory if it does not exist
[[ -d $log_dir ]] || mkdir -p $log_dir && chown $user:$group $log_dir

touch $log_dir/wavefront-daemon.log
touch $log_dir/wavefront-error.log
chown $user:$group $log_dir/wavefront-daemon.log
chown $user:$group $log_dir/wavefront-error.log
chmod 644 $log_dir/wavefront-daemon.log
chmod 644 $log_dir/wavefront-error.log

# Configure agent to start on reboot.
if [[ -f /etc/debian_version ]]; then
	update-rc.d $service_name defaults 99
elif [[ -f /etc/redhat-release ]] || [[ -f /etc/system-release-cpe ]]; then
	chkconfig --level 345 $service_name on
elif [[ -f /etc/SUSE-brand ]]; then
        insserv $service_name
        systemctl enable $service_name
fi

# Allow system user to write .wavefront_id/buffer files to install dir.
chown $user:$group $wavefront_dir/$service_name
chown $user:$group $conf_dir/$service_name

if [[ ! -f $conf_dir/$service_name/wavefront.conf ]]; then
    if [[ -f $wavefront_dir/$service_name/conf/wavefront.conf ]]; then
        echo "Copying $conf_dir/$service_name/wavefront.conf from $wavefront_dir/$service_name/conf/wavefront.conf" >&2
        cp $wavefront_dir/$service_name/conf/wavefront.conf $conf_dir/$service_name/wavefront.conf
    else
        echo "Creating $conf_dir/$service_name/wavefront.conf from default template" >&2
        cp $conf_dir/$service_name/wavefront.conf.default $conf_dir/$service_name/wavefront.conf
    fi
else
   echo "$conf_dir/$service_name/wavefront.conf already exists"
fi

if [[ ! -f $conf_dir/$service_name/preprocessor_rules.yaml ]]; then
    echo "Creating $conf_dir/$service_name/preprocessor_rules.yaml from default template" >&2
    cp $conf_dir/$service_name/preprocessor_rules.yaml.default $conf_dir/$service_name/preprocessor_rules.yaml
fi

if [[ ! -f $conf_dir/$service_name/log4j2.xml ]]; then
    echo "Creating $conf_dir/$service_name/log4j2.xml from default template" >&2
    cp $conf_dir/$service_name/log4j2.xml.default $conf_dir/$service_name/log4j2.xml
fi


# If there is an errant pre-3.9 agent running, we need to kill it. This is
# required for a clean upgrade from pre-3.9 to 3.9+.
old_pid_file="/var/run/wavefront.pid"
if [[ -f $old_pid_file ]]; then
	pid=$(cat $old_pid_file)
	kill -9 "$pid" || true
	rm $old_pid_file
fi

# Stop the 3.24/4.1 service if was started during boot, since it is running with a different .pid file.
old_pid_file="/var/run/S99wavefront-proxy.pid"
if [[ -f $old_pid_file ]]; then
    export PID_FILE=$old_pid_file
    if [[ -f /etc/rc2.d/S99wavefront-proxy ]]; then
        /etc/rc2.d/S99wavefront-proxy stop || true
    fi
    if [[ -f /etc/rc.d/rc2.d/S99wavefront-proxy ]]; then
        /etc/rc.d/rc2.d/S99wavefront-proxy stop || true
    fi
    # if stopping didn't work, we'll have to kill the process
    if [[ -f $old_pid_file ]]; then
        pid=$(cat $old_pid_file)
	    kill -9 "$pid" || true
	    rm $old_pid_file
	fi
fi

[[ -d $jre_dir ]] || mkdir -p $jre_dir

service $service_name condrestart

exit 0

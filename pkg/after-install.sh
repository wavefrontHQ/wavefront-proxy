#!/bin/bash -ex
# These variables should match default values in /etc/init.d/wavefront-proxy
user="wavefront"
group="wavefront"
service_name="wavefront-proxy"
spool_dir="/var/spool/wavefront-proxy"
wavefront_dir="/opt/wavefront"
conf_dir="/etc/wavefront"
log_dir="/var/log/wavefront"

# Set up wavefront user.
if ! groupmod $group &> /dev/null; then
	groupadd $group &> /dev/null
fi
if ! id $user &> /dev/null; then
	useradd -r -s /bin/bash -g $group $user &> /dev/null
fi

# Create spool directory if it does not exist.
[[ -d $spool_dir ]] || mkdir -p $spool_dir && chown $user:$group $spool_dir
[[ -d $spool_dir/buffer ]] || mkdir -p $spool_dir/buffer && chown $user:$group $spool_dir/buffer

# Create log directory if it does not exist
[[ -d $log_dir ]] || mkdir -p $log_dir && chown $user:$group $log_dir

# Allow system user to write .wavefront_id/buffer files to install dir.
chown $user:$group $wavefront_dir/$service_name
chown $user:$group $conf_dir/$service_name

if [[ ! -f $conf_dir/$service_name/wavefront.conf ]]; then
    if [[ -f $wavefront_dir/$service_name/conf/wavefront.conf ]]; then
        echo "Copying $conf_dir/$service_name/wavefront.conf from $wavefront_dir/$service_name/conf/wavefront.conf"
        cp $wavefront_dir/$service_name/conf/wavefront.conf $conf_dir/$service_name/wavefront.conf
    else
        echo "Creating $conf_dir/$service_name/wavefront.conf from default template"
        cp $conf_dir/$service_name/wavefront.conf.default $conf_dir/$service_name/wavefront.conf
    fi
fi

if [[ ! -f $conf_dir/$service_name/preprocessor_rules.yaml ]]; then
    echo "Creating $conf_dir/$service_name/preprocessor_rules.yaml from default template"
    cp $conf_dir/$service_name/preprocessor_rules.yaml.default $conf_dir/$service_name/preprocessor_rules.yaml
fi

if [[ ! -f $conf_dir/$service_name/log4j2.xml ]]; then
    echo "Creating $conf_dir/$service_name/log4j2.xml from default template"
    cp $conf_dir/$service_name/log4j2.xml.default $conf_dir/$service_name/log4j2.xml
fi

systemctl enable -q ${service_name}

exit 0

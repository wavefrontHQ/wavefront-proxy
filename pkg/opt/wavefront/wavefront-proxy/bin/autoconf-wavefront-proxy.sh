#!/bin/bash
#
# This script may automatically configure wavefront without prompting, based on
# these variables:
#  WAVEFRONT_URL
#  WAVEFRONT_HOSTNAME
#  WAVEFRONT_USE_GRAPHITE
#  WAVEFRONT_TOKEN


APP_BASE=wavefront
APP_HOME=/opt/$APP_BASE/$APP_BASE-proxy
CONF_FILE=$APP_HOME/conf/$APP_BASE.conf
DEFAULT_URL=https://metrics.wavefront.com/api/
DEFAULT_HOSTNAME=`hostname`

function error() {
	echo
	echo "  ERROR: $1"
	echo
}

function get_input() {
	# get_input <prompt> [ <default_value> ]
	default_value=$2
	if [ -n "$default_value" ]; then
		prompt="$1 (default: $default_value)"
	else
		prompt=$1
	fi
	user_input=""
	while [ -z "$user_input" ]; do
		echo $prompt
		read user_input
		if [ -z "$user_input" ] && [ -n "$default_value" ]; then
			user_input=$default_value
		fi
		if [ -n "$user_input" ]; then
			if [[ $user_input == *","* ]]; then
				error "The value cannot contain commas (,)."
				user_input=""
			fi
		else
			error "The value cannot be blank."
		fi
	done
}

if [[ -z $WAVEFRONT_URL ]]; then
	get_input "1) Please enter the server URL:" $DEFAULT_URL
	WAVEFRONT_URL=$user_input
fi
echo "Setting server=$WAVEFRONT_URL"
if grep -q ^#server $CONF_FILE; then
	sed -ri s,^#server.*,server=$WAVEFRONT_URL,g $CONF_FILE
else
	sed -ri s,^server.*,server=$WAVEFRONT_URL,g $CONF_FILE
fi

if [[ -z $WAVEFRONT_TOKEN ]]; then
	get_input "2) Please enter a valid API token:"
	WAVEFRONT_TOKEN=$user_input
fi
echo "Setting token=$WAVEFRONT_TOKEN"
if grep -q ^#token $CONF_FILE; then
	sed -ri s,^#token.*,token=$WAVEFRONT_TOKEN,g $CONF_FILE
else
	sed -ri s,^token.*,token=$WAVEFRONT_TOKEN,g $CONF_FILE
fi

if [[ -z $WAVEFRONT_HOSTNAME ]]; then
	get_input "3) Please enter the hostname:" $DEFAULT_HOSTNAME
	WAVEFRONT_HOSTNAME=$user_input
fi

echo "Setting hostname=$WAVEFRONT_HOSTNAME"
if grep -q ^#hostname $CONF_FILE; then
	sed -ri s,^#hostname.*,hostname=$WAVEFRONT_HOSTNAME,g $CONF_FILE
else
	sed -ri s,^hostname.*,hostname=$WAVEFRONT_HOSTNAME,g $CONF_FILE
fi

response=""
if [[ -n $WAVEFRONT_USE_GRAPHITE ]]; then
	response=$WAVEFRONT_USE_GRAPHITE
fi

while [ -z "$response" ]; do
	read -p "4) Would you like to use Graphite? [y/n] " -n 1 -r response
	echo
	if [[ ! $response =~ ^[YyNn]$ ]]; then
		error "'$response' is not a valid response."
		response=""
	fi
done
if [[ $response =~ ^[Yy]$ ]]; then
	echo "Enabling Graphite setings"
	sed -ri 's,^#graphitePorts(.*),graphitePorts\1,g' $CONF_FILE
	sed -ri 's,^#graphiteFormat(.*),graphiteFormat\1,g' $CONF_FILE
	sed -ri 's,^#graphiteDelimiters(.*),graphiteDelimiters\1,g' $CONF_FILE
	/etc/init.d/$APP_BASE-proxy restart
else
	echo "Disabling Graphite settings"
	sed -ri 's,^graphitePorts(.*),#graphitePorts\1,g' $CONF_FILE
	sed -ri 's,^graphiteFormat(.*),#graphiteFormat\1,g' $CONF_FILE
	sed -ri 's,^graphiteDelimiters(.*),#graphiteDelimiters\1,g' $CONF_FILE
	/etc/init.d/$APP_BASE-proxy restart
fi

exit 0

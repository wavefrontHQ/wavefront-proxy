#/bin/sh
# This should be the full path to the agent program, including the /bin directory; no trailing slash necessary
DAEMON_PATH="/opt/wavefront/push-agent-2.0/bin"
# The name of the jar
DAEMON=wavefront-push-agent-2.0.jar
# Name of the log file
NAME=wavefront
# This should be the full path to the configuration file.
CONFIG_FILE="/opt/wavefront/push-agent-2.0/conf/wavefront.conf"
# Java options for memory, GC, etc.
JAVA_OPTS="-Xmx4G"

# Run
cd $DAEMON_PATH
java $JAVA_OPTS -jar $DAEMON -f $CONFIG_FILE >> /var/log/$NAME.log 2>&1

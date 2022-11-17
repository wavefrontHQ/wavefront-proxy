#!/bin/bash -e

proxy_dir=${PROXY_DIR:-/opt/wavefront/wavefront-proxy}
config_dir=${CONFIG_DIR:-/etc/wavefront/wavefront-proxy}

service_name="wavefront-proxy"
sysconfig="/etc/sysconfig/$service_name"
[[ -f "$sysconfig" ]] && . $sysconfig

badConfig() {
    echo "Proxy configuration incorrect"
    echo "setup 'server' and 'token' in '${config_file}' file."
    exit -1 
}

setupEnv(){
    if [ -n "${PROXY_JAVA_HOME}" ]; then
        echo "using JRE in `${PROXY_JAVA_HOME}`(PROXY_JAVA_HOME) as JAVA_HOME"
        JAVA_HOME = ${PROXY_JAVA_HOME}
    else
        if [ -n "${JAVA_HOME}" ]; then
            echo "using JRE in \"${JAVA_HOME}\" (JAVA_HOME)"
        else
            JAVA_HOME=$(readlink -f $(which java) | sed "s:/bin/java::")
            if [ -d "${JAVA_HOME}" ]; then
                echo "using JRE in \"${JAVA_HOME}\" ($(which java))"
            else 
                echo "Error! JAVA_HOME (or PROXY_JAVA_HOME) not defined, use `${sysconfig}` file to define it"
                exit -1
            fi
        fi
    fi

    config_file=${config_dir}/wavefront.conf
    echo "Using \"${config_file}\" as config file"
    grep -q CHANGE_ME ${config_file} && badConfig

    log4j2_file=${config_dir}/log4j2.xml
    echo "Using \"${log4j2_file}\" as log config file"

    if [ -z "$STDOUT_LOG" ]; then STDOUT_LOG="/var/log/wavefront/wavefront_stdout.log"; fi
    echo "Using \"${STDOUT_LOG}\" as stdout log file"

    proxy_jar=${AGENT_JAR:-$proxy_dir/bin/wavefront-proxy.jar}

    # If JAVA_ARGS is not set, try to detect memory size and set heap to 8GB if machine has more than 8GB.
    # Fall back to using AggressiveHeap (old behavior) if less than 8GB.
    if [ -z "$JAVA_ARGS" ]; then
        if [ `grep MemTotal /proc/meminfo | awk '{print $2}'` -gt "8388607" ]; then
            java_args=-Xmx8g
            >&2 echo "Using default heap size (8GB), please set JAVA_ARGS in '${sysconfig}' to use a different value"
        else
            java_args=-XX:+AggressiveHeap
        fi
    else
        java_args=$JAVA_ARGS
    fi
}

setupEnv

${JAVA_HOME}/bin/java \
    $java_args \
    -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager \
    -Dlog4j.configurationFile=${log4j2_file} \
    -jar $proxy_jar \
    -f $config_file \
    $APP_ARGS >> ${STDOUT_LOG} 2>&1
    


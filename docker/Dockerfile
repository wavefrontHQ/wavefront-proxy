# NOTE: we need this to be a Dockerfile because that's the only option
# when using the Automated Build Service for Red Hat Build Partners.
ARG BASE=adoptopenjdk:11-jre-hotspot
FROM ${BASE}

ARG BASE

# This script may automatically configure wavefront without prompting, based on
# these variables:
#  WAVEFRONT_URL           (required)
#  WAVEFRONT_TOKEN         (required)
#  JAVA_HEAP_USAGE         (default is 4G)
#  WAVEFRONT_HOSTNAME      (default is the docker containers hostname)
#  WAVEFRONT_PROXY_ARGS    (default is none)
#  JAVA_ARGS               (default is none)

LABEL \
    maintainer="wavefront@vmware.com" \
    name="Wavefront Collector" \
    vendor="Wavefront by VMware" \
    version="10.12" \
    release="10.12" \
    summary="The Wavefront Proxy is a light-weight Java application that you send your metrics, histograms, and trace data to. It handles batching and transmission of your data to the Wavefront service in a secure, fast, and reliable manner." \
    description="The Wavefront Proxy is a light-weight Java application that you send your metrics, histograms, and trace data to. It handles batching and transmission of your data to the Wavefront service in a secure, fast, and reliable manner."

# install java on RHEL
RUN case "${BASE}" in \
    *redhat*) \
    yum-config-manager --enable rhel-7-server-optional-rpms \
    && yum update --disableplugin=subscription-manager -y \
    && rm -rf /var/cache/yum \
    && yum install -y sudo curl hostname java-11-openjdk-devel \
    ;; \
    esac

# Add new group:user "wavefront"
RUN useradd wavefront
RUN mkdir -p /var/spool/wavefront-proxy
RUN chown -R wavefront:wavefront /var/spool/wavefront-proxy

RUN case "${BASE}" in \
    *redhat*) chown -R wavefront:wavefront /usr/lib/jvm/java-11-openjdk/lib/security/cacerts ;; \
    *) chown -R wavefront:wavefront /opt/java/openjdk/lib/security/cacerts ;; \
    esac

EXPOSE 3878
EXPOSE 2878
EXPOSE 4242

USER wavefront:wavefront

COPY wavefront-proxy.jar /opt/wavefront/wavefront-proxy/wavefront-proxy.jar
COPY run.sh /opt/wavefront/wavefront-proxy/run.sh
COPY log4j2.xml /etc/wavefront/wavefront-proxy/log4j2.xml
COPY LICENSE /licenses

CMD ["/bin/bash", "/opt/wavefront/wavefront-proxy/run.sh"]

FROM eclipse-temurin:11.0.13_8-jre

# This script may automatically configure wavefront without prompting, based on
# these variables:
#  WAVEFRONT_URL           (required)
#  WAVEFRONT_TOKEN         (required)
#  JAVA_HEAP_USAGE         (default is 4G)
#  WAVEFRONT_HOSTNAME      (default is the docker containers hostname)
#  WAVEFRONT_PROXY_ARGS    (default is none)
#  JAVA_ARGS               (default is none)

# Add new group:user "wavefront"
RUN groupadd -g 2000 wavefront
RUN useradd --uid 1000 --gid 2000 -m wavefront
RUN chown -R wavefront:wavefront /opt/java/openjdk/lib/security/cacerts
RUN mkdir -p /var/spool/wavefront-proxy
RUN chown -R wavefront:wavefront /var/spool/wavefront-proxy

# Run the agent
EXPOSE 3878
EXPOSE 2878
EXPOSE 4242

USER wavefront:wavefront

ADD wavefront-proxy.jar /opt/wavefront/wavefront-proxy/wavefront-proxy.jar
ADD run.sh /opt/wavefront/wavefront-proxy/run.sh
ADD log4j2.xml /etc/wavefront/wavefront-proxy/log4j2.xml

CMD ["/bin/bash", "/opt/wavefront/wavefront-proxy/run.sh"]

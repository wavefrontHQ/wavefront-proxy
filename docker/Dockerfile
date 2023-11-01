FROM harbor-repo.vmware.com/dockerhub-proxy-cache/library/maven:3.9.0-eclipse-temurin-11 as compile
COPY . proxy
RUN cd proxy; mvn -f proxy clean install -DskipTests -DskipFormatCode

FROM harbor-repo.vmware.com/dockerhub-proxy-cache/library/eclipse-temurin:11

# Wavefront authentication can be configured in three different ways: Customers that have been
# onboarded by CSP can set up CSP api tokens or CSP OAuth apps (CSP_APP_ID, CSP_APP_SECRET).
# Customers of Wavefront can use Wavefront api token. This script may automatically
# configure Wavefront without prompting, based on these variables:
#  WAVEFRONT_URL           (required)
#  WAVEFRONT_TOKEN         (not required)
#  CSP_API_TOKEN           (not required)
#  CSP_APP_ID              (not required)
#  CSP_APP_SECRET          (not required)
#  CSP_ORG_ID              (not required)
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

RUN mkdir -p /var/log/wavefront
RUN chown -R wavefront:wavefront /var/log/wavefront

# Run the agent
EXPOSE 3878
EXPOSE 2878
EXPOSE 4242

USER wavefront:wavefront

COPY --from=compile proxy/proxy/target/proxy-13.2-spring-boot.jar /opt/wavefront/wavefront-proxy/wavefront-proxy.jar
ADD docker/create-lemans-buffered-pipelines.sh /opt/wavefront/create-lemans-buffered-pipelines.sh
ADD docker/run.sh /opt/wavefront/wavefront-proxy/run.sh
ADD docker/log4j2.xml /etc/wavefront/wavefront-proxy/log4j2.xml
ADD docker/LICENSE /licenses/LICENSE

CMD ["/bin/bash", "/opt/wavefront/wavefront-proxy/run.sh"]

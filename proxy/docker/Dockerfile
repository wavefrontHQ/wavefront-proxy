FROM ubuntu:14.04

# This script may automatically configure wavefront without prompting, based on
# these variables:
#  WAVEFRONT_URL           (required)
#  WAVEFRONT_TOKEN         (required)
#  JAVA_HEAP_USAGE         (default is 4G)
#  WAVEFRONT_HOSTNAME      (default is the docker containers hostname)
#  WAVEFRONT_USE_GRAPHITE  (default is false)

# Dumb-init
RUN apt-get -y update
RUN apt-get install -y curl
RUN curl -SLO https://github.com/Yelp/dumb-init/releases/download/v1.1.3/dumb-init_1.1.3_amd64.deb
RUN dpkg -i dumb-init_*.deb
RUN rm dumb-init_*.deb
ENTRYPOINT ["/usr/bin/dumb-init", "--"]

# Download wavefront. Merely extract the debian, don't want to try running startup scripts.
RUN curl -SLO https://packagecloud.io/wavefront/proxy/packages/ubuntu/trusty/wavefront-proxy_4.1-1_amd64.deb/download
RUN dpkg -x download /
RUN rm download
# Download and install JRE, since it's no longer bundled with the proxy
RUN mkdir /opt/wavefront/wavefront-proxy/jre
RUN curl -s https://s3-us-west-2.amazonaws.com/wavefront-misc/proxy-jre.tgz | tar -xz --strip 1 -C /opt/wavefront/wavefront-proxy/jre

# Clean up APT when done.
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Configure agent
ENV DO_SERVICE_RESTART=false
RUN cp /etc/wavefront/wavefront-proxy/wavefront.conf.default /etc/wavefront/wavefront-proxy/wavefront.conf
RUN cp /etc/wavefront/wavefront-proxy/log4j2-stdout.xml.default /etc/wavefront/wavefront-proxy/log4j2.xml
RUN echo '\nephemeral=true' >> /etc/wavefront/wavefront-proxy/wavefront.conf
RUN echo '\nflushThreads=6' >> /etc/wavefront/wavefront-proxy/wavefront.conf

# Run the agent
EXPOSE 3878
EXPOSE 2878
EXPOSE 4242

ENV PATH=/opt/wavefront/wavefront-proxy/jre/bin:$PATH
ENV WAVEFRONT_USE_GRAPHITE=false
ADD run.sh run.sh
CMD ["/bin/bash", "/run.sh"]

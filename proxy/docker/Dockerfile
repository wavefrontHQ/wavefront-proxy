FROM ubuntu:14.04

# This script may automatically configure wavefront without prompting, based on
# these variables:
#  WAVEFRONT_URL           (required)
#  WAVEFRONT_TOKEN         (required)
#  JAVA_HEAP_USAGE         (defualt is 4G)
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
RUN curl -SLO https://packagecloud.io/wavefront/proxy/packages/ubuntu/trusty/wavefront-proxy_3.24-1_amd64.deb/download
RUN dpkg -x download /
RUN rm download

# Clean up APT when done.
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Configure agent
ENV DO_SERVICE_RESTART=false
RUN echo '\nephemeral=true' >> /opt/wavefront/wavefront-proxy/conf/wavefront.conf

# Run the agent
EXPOSE 3878
EXPOSE 2878
EXPOSE 4242

ENV PATH=/opt/wavefront/wavefront-proxy/jre/bin:$PATH
ENV WAVEFRONT_USE_GRAPHITE=false
ADD run.sh run.sh
CMD ["/bin/bash", "/run.sh"]

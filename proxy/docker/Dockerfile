FROM ubuntu:18.04

# This script may automatically configure wavefront without prompting, based on
# these variables:
#  WAVEFRONT_URL           (required)
#  WAVEFRONT_TOKEN         (required)
#  JAVA_HEAP_USAGE         (default is 4G)
#  WAVEFRONT_HOSTNAME      (default is the docker containers hostname)
#  WAVEFRONT_PROXY_ARGS    (default is none)
#  JAVA_ARGS               (default is none)

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get -y update && apt-get install -y \
  dialog \
  apt-transport-https \
  apt-utils \
  curl \
  sudo \
  gnupg2 \
  debian-archive-keyring \
  apt-transport-https \
  openjdk-11-jdk

# Download wavefront proxy (latest release). Merely extract the debian, don't want to try running startup scripts.
RUN echo "deb https://packagecloud.io/wavefront/proxy/ubuntu/ bionic main" > /etc/apt/sources.list.d/wavefront_proxy.list
RUN echo "deb-src https://packagecloud.io/wavefront/proxy/ubuntu/ bionic main" >> /etc/apt/sources.list.d/wavefront_proxy.list

RUN curl -Lo packagecloud.key "https://packagecloud.io/wavefront/proxy/gpgkey" 2>&1
RUN APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1 apt-key add packagecloud.key
RUN apt-get -y update

RUN apt-get -d install wavefront-proxy
RUN dpkg -x $(ls /var/cache/apt/archives/wavefront-proxy* | tail -n1) /

# Clean up APT when done.
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Configure agent
RUN cp /etc/wavefront/wavefront-proxy/log4j2-stdout.xml.default /etc/wavefront/wavefront-proxy/log4j2.xml
ADD proxy-9.4-SNAPSHOT-uber.jar /opt/wavefront/wavefront-proxy/bin/wavefront-proxy.jar

# Add new group: wavefront
RUN groupadd -g 2000 wavefront

# Add new user: wavefront
RUN adduser --disabled-password --gecos '' --uid 1000 --gid 2000 wavefront
RUN chown -R wavefront:wavefront /var
RUN chmod 755 /var

USER 1000:2000

# Run the agent
EXPOSE 3878
EXPOSE 2878
EXPOSE 4242

ADD run.sh run.sh
CMD ["/bin/bash", "/run.sh"]

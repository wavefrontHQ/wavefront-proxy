FROM phusion/baseimage:0.9.16

# Set correct environment variables.
ENV HOME /root

# Regenerate SSH host keys. baseimage-docker does not contain any, so you
# have to do that yourself. You may also comment out this instruction; the
# init system will auto-generate one during boot.
RUN /etc/my_init.d/00_regen_ssh_host_keys.sh

# Use baseimage-docker's init system.
CMD ["/sbin/my_init"]

# Add Java 8
RUN add-apt-repository ppa:webupd8team/java
RUN apt-get update
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
RUN apt-get install -y oracle-java8-installer oracle-java8-set-default

# Clean up APT when done.
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Download production agent from S3
RUN wget https://wavefront-customer.s3.amazonaws.com/20141106-wavefront-agent-2.0.tgz

# Install production agent
RUN mkdir -p /opt/wavefront && cd /opt/wavefront && tar -zxvf /20141106-wavefront-agent-2.0.tgz

# Configure agent
ADD wavefront.conf wavefront.conf
RUN echo >> /opt/wavefront/push-agent-2.0/conf/wavefront.conf && cat wavefront.conf >> /opt/wavefront/push-agent-2.0/conf/wavefront.conf

# Run the agent
COPY run.sh run.sh
CMD ["/bin/sh", "run.sh"]

# Create a docker VM for building the wavefront proxy agent .deb and .rpm
# packages.
FROM ruby

RUN gem install fpm
RUN apt-get update
RUN apt-get install -y openjdk-11-jdk rpm

RUN git clone --depth 1 --branch commons-daemon-1.2.4 https://github.com/apache/commons-daemon /opt/commons-daemon
RUN cd /opt/commons-daemon/src/native/unix && support/buildconf.sh && ./configure --with-java=/usr/lib/jvm/java-11-openjdk-amd64 && make

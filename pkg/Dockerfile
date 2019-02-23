# Create a docker VM for building the wavefront proxy agent .deb and .rpm
# packages.
FROM centos:6
RUN yum -y install gcc make autoconf wget vim rpm-build git gpg2

# Set up Ruby 1.9.3 for FPM.
RUN gpg2 --keyserver hkp://keys.gnupg.net --recv-keys 409B6B1796C275462A1703113804BB82D39DC0E3 7D2BAF1CF37B13E2069D6956105BD0E739499BDB --homedir /root/.gnupg
RUN curl -L get.rvm.io | bash -s stable
ENV PATH /usr/local/rvm/gems/ruby-1.9.3-p551/bin:/usr/local/rvm/gems/ruby-1.9.3-p551@global/bin:/usr/local/rvm/rubies/ruby-1.9.3-p551/bin:/usr/local/rvm/bin:$PATH
ENV LC_ALL en_US.UTF-8
RUN rvm install 1.9.3
RUN gem install fpm --version 1.10.0
RUN gem install package_cloud --version 0.2.35

# Wavefront software. This repo contains build scripts for the agent, to be run
# inside this container.
RUN git clone https://github.com/wavefrontHQ/java.git /opt/wavefront-java-repo
# Uncomment the line below to test local changes.
# ADD . /opt/wavefront-java-repo/pkg

# Apache commons daemon. Built from source.
RUN git clone https://github.com/apache/commons-daemon /opt/commons-daemon

# The person building the package will need to copy a JDK and a wavefront proxy
# JAR into the container and then invoke the build scripts.

# Create a docker VM for building the wavefront proxy agent .deb and .rpm
# packages.
#
# Resulting packages have been tested on RHEL 6/7, and Ubuntu 14.04.
FROM centos:6
RUN yum -y install gcc make autoconf wget vim rpm-build

# Set up Ruby 1.9.3 for FPM.
RUN gpg --keyserver hkp://keys.gnupg.net --recv-keys 409B6B1796C275462A1703113804BB82D39DC0E3
RUN curl -L get.rvm.io | bash -s stable
ENV PATH /usr/local/rvm/gems/ruby-1.9.3-p551/bin:/usr/local/rvm/gems/ruby-1.9.3-p551@global/bin:/usr/local/rvm/rubies/ruby-1.9.3-p551/bin:/usr/local/rvm/bin:$PATH
RUN rvm install 1.9.3
RUN gem install fpm

# Wavefront software.
RUN yum -y install git
RUN git clone https://github.com/wavefrontHQ/java.git /root/java

# JDK to bundle with packages.
RUN wget http://cdn.azul.com/zulu/bin/zulu8.13.0.5-jdk8.0.72-linux_x64.tar.gz
RUN tar xvf zulu8.13.0.5-jdk8.0.72-linux_x64.tar.gz
RUN rm zulu8.13.0.5-jdk8.0.72-linux_x64.tar.gz
RUN mv zulu8.13.0.5-jdk8.0.72-linux_x64 zulu-jdk
ENV JAVA_HOME /zulu-jdk

# Maven
RUN wget http://mirror.cc.columbia.edu/pub/software/apache/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz
RUN tar xzf apache-maven-3.0.5-bin.tar.gz -C /usr/local
RUN rm apache-maven-3.0.5-bin.tar.gz
RUN cd /usr/local && ln -s apache-maven-3.0.5 maven
RUN echo 'export M2_HOME=/usr/local/maven' >> /etc/profile.d/maven.sh
RUN echo 'export PATH=${M2_HOME}/bin:${PATH}' >> /etc/profile.d/maven.sh

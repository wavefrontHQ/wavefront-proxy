# Create a docker VM for building the wavefront proxy agent .deb and .rpm
# packages.
FROM ruby:3.0

RUN gem install fpm
RUN gem install package_cloud
RUN apt-get update
RUN apt-get install -y rpm

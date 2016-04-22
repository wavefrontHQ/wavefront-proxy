Wavefront Proxy Packaging
=========================

Tools
-----
* Install [docker](https://www.docker.com/).

Methodology
-----------
Build and run the docker container for building.

    # Build docker buildbox.
    cd java/pkg
    docker build -t wavefront-proxy-builder .

    # Build WF jar to ship to users.
    cd ../proxy
    mvn package
    cd ../pkg

    docker run -it wavefront-proxy-builder bash
    docker cp ../proxy/target/wavefront-push-agent.jar <my_docker_image>:/root
    # Inside docker container
    cd /root/java/proxy
    mvn package
    cd /root/java/pkg
    ./stage.sh /zulu-jdk /commons-daemon ~/wavefront ../target/wavefront-push-agent.jar
    ./build.sh deb 3.1 4

    # Outside docker container
    docker cp my_container:/root/java/pkg/wavefront-proxy_3.8-1_amd64.deb .

This will build the agent from head and package it into a deb or an rpm. The agent will use whatever JRE
you bundle with it -- Wavefront uses Zulu 8.11.0.1 a8c3eea6250f.

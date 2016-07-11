Wavefront Proxy Packaging
=========================

Tools
-----
* Install [docker](https://www.docker.com/).
* Install Java 7+/Maven.

Methodology
-----------
Build and run the docker container for building.

    ### Outside docker container
    docker pull wavefronthq/proxy-builder
    docker run -it wavefronthq/proxy-builder bash
    # Copy JRE into docker container for building WF proxy
    docker cp <my_jre_directory> my_container:/opt/jre
    # Copy a WF proxy that you build into the docker container
    mvn package -pl proxy -am
    docker cp proxy/target/wavefront-push-agent.jar my_container:/opt

    ### Inside docker container
    cd /opt/wavefront-java-repo/pkg
    git pull
    ./stage.sh /opt/jre /opt/commons-daemon /opt/wavefront-push-agent.jar
    ./build.sh deb 3.1 4

    # Outside docker container
    docker cp my_container:/root/java/pkg/wavefront-proxy_3.8-1_amd64.deb .

This will build the agent from head and package it into a deb or an rpm. The agent will use whatever JRE
you bundle with it -- Wavefront uses Zulu 8.11.0.1 a8c3eea6250f.

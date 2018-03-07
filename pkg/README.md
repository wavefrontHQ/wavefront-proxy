Wavefront Proxy Packaging
=========================

Tools
-----
* Install [docker](https://www.docker.com/).
* Install Java 8+/Maven.

Methodology
-----------
Build and run the docker container for building.

    ### Outside docker container
    docker pull wavefronthq/proxy-builder
    docker run -it wavefronthq/proxy-builder bash
    container_id=`docker ps -f ancestor=wavefronthq/proxy-builder -f status=running -n 1 -q`
    docker exec $container_id mkdir /opt/jre
    # Copy JRE into docker container for building WF proxy
    docker cp <my_jre_directory> $container_id:/opt/jre
    # Copy a WF proxy that you build into the docker container
    mvn package -pl proxy -am
    docker cp proxy/target/proxy-3.99-SNAPSHOT-uber.jar $container_id:/opt/wavefront-push-agent.jar

    ### Inside docker container
    yum upgrade
    cd /opt/wavefront-java-repo/pkg
    git pull
    ./stage.sh /opt/jre /opt/commons-daemon /opt/wavefront-push-agent.jar
    ./build.sh deb 3.99 4

    # Outside docker container
    docker cp $container_id:/opt/wavefront-java-repo/pkg/out/wavefront-proxy_3.99-4_amd64.deb .

This will build the agent from head and package it into a deb or an rpm. The proxy daemon script will download and install Zulu 8.13.0.5 0f21d10ca4f1 JRE by default if no JRE is found in /opt/wavefront/wavefront-proxy/proxy-jre on proxy startup, so to bundle the package with a JRE of your choice instead, run the following command inside the docker container right before build.sh:

    cp -r /opt/jre /opt/wavefront-java-repo/pkg/build/opt/wavefront/wavefront-proxy/proxy-jre


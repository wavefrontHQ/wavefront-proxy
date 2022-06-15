FROM bitnami/telegraf:latest AS build

FROM eclipse-temurin:11

## FROM wavefront-proxy/docke

COPY --from=build /opt/bitnami/telegraf/bin/telegraf /bin/telegraf

RUN groupadd -g 2000 wavefront
RUN useradd --uid 1000 --gid 2000 -m wavefront
RUN chown -R wavefront:wavefront /opt/java/openjdk/lib/security/cacerts
RUN mkdir -p /var/spool/wavefront-proxy
RUN chown -R wavefront:wavefront /var/spool/wavefront-proxy

USER wavefront:wavefront

ADD wavefront-proxy.jar /opt/wavefront/wavefront-proxy/wavefront-proxy.jar
ADD run.sh /opt/wavefront/wavefront-proxy/run.sh
ADD log4j2.xml /etc/wavefront/wavefront-proxy/log4j2.xml
ADD telegraf.conf /etc/telegraf/telegraf.conf

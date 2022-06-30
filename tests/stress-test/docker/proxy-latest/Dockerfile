FROM bitnami/telegraf:latest AS build

FROM wavefronthq/proxy:latest

## FROM wavefront-proxy/docke

COPY --from=build /opt/bitnami/telegraf/bin/telegraf /bin/telegraf

ADD run.sh /opt/wavefront/wavefront-proxy/run.sh
ADD log4j2.xml /etc/wavefront/wavefront-proxy/log4j2.xml
ADD telegraf.conf /etc/telegraf/telegraf.conf

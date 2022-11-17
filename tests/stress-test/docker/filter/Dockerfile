FROM node

ADD metrics_filter /opt/wf/
ADD certs /opt/certs/


EXPOSE 8000/tcp
EXPOSE 8001/tcp

WORKDIR /opt/wf/
CMD ["node", "filter.js", "0"]
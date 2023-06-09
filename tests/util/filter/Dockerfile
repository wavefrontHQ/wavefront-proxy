FROM node:18.3

ADD filter.js /opt/filter/
ADD index.html /opt/filter/
ADD package.json /opt/filter/
ADD rootCA.key /opt/filter/
ADD rootCA.pem /opt/filter/

WORKDIR /opt/filter/
RUN npm install

ENTRYPOINT node filter.js
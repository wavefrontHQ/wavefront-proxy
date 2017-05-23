The Proxy will accept Wavefront formatted message on port 2878 and OpenTSDB formatted messages on port 4242. Just run this docker image with the following environment variabled defined, e.g. 

    docker build -t wavefront-proxy .
    docker run \
        -e WAVEFRONT_URL=https://you.wavefront.com/api \
        -e WAVEFRONT_TOKEN=<YOUR-API-TOKEN> \
        -p 2878:2878 \
        -p 4242:4242 \
        wavefront-proxy

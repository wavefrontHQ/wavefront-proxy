Just run this docker image with the following environment variabled defined, e.g.

    docker build -t wavefront-proxy .
    docker run \
        -e WAVEFRONT_URL=https://you.wavefront.com/api \
        -e WAVEFRONT_TOKEN=63698a5f-deea-4a9c-ae6c-4034acd75d55 \
        wavefront-proxy

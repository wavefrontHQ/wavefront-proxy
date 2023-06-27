## Build

    docker build -t wavefront-proxy .

## run

The Proxy will accept Wavefront formatted message on port 2878 (additional listeners can be enabled in WAVEFRONT_PROXY_ARGS, see below). 
Just run this docker image with the following environment variables defined, e.g. 

#### WF Token

    docker run \
        -e WAVEFRONT_URL=https://you.wavefront.com/api \
        -e WAVEFRONT_TOKEN=<YOUR-API-TOKEN> \
        -p 2878:2878 \
        wavefront-proxy

#### CSP App Secret

    docker run -d \
        -e WAVEFRONT_URL=https://you.wavefront.com/api/ \
        -e CSP_APP_ID=<CSP_APP_ID> \
        -e CSP_APP_SECRET=<CSP_APP_SECRET> \
        -p 2878:2878 \
        wavefront-proxy

#### CSP App Secret and ORG Id

    docker run -d \
        -e WAVEFRONT_URL=https://you.wavefront.com/api/ \
        -e CSP_APP_ID=<CSP_APP_ID> \
        -e CSP_APP_SECRET=<CSP_APP_SECRET> \
        -e CSP_ORG_ID=<CSP_ORG_ID> \
        -p 2878:2878 \
        wavefront-proxy

#### CSP Api Token

    docker run -d \
        -e WAVEFRONT_URL=https://you.wavefront.com/api/ \
        -e CSP_API_TOKEN=<CSP_API_TOKEN> \
        -p 2878:2878 \
        wavefront-proxy

## Configuration

All properties that exist in [wavefront.conf](https://github.com/wavefrontHQ/java/blob/master/pkg/etc/wavefront/wavefront-proxy/wavefront.conf.default) can be customized by passing their name as long form arguments within your docker run command in the WAVEFRONT_PROXY_ARGS environment variable. For example, add `-e WAVEFRONT_PROXY_ARGS="--pushRateLimit 1000"` to your docker run command to specify a [rate limit](https://github.com/wavefrontHQ/java/blob/master/pkg/etc/wavefront/wavefront-proxy/wavefront.conf.default#L62) of 1000 pps for the proxy.

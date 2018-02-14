The Proxy will accept Wavefront formatted message on port 2878 and OpenTSDB formatted messages on port 4242. Just run this docker image with the following environment variabled defined, e.g. 

    docker build -t wavefront-proxy .
    docker run \
        -e WAVEFRONT_URL=https://you.wavefront.com/api \
        -e WAVEFRONT_TOKEN=<YOUR-API-TOKEN> \
        -p 2878:2878 \
        -p 4242:4242 \
        wavefront-proxy

All properties that exist in [wavefront.conf](https://github.com/wavefrontHQ/java/blob/master/pkg/etc/wavefront/wavefront-proxy/wavefront.conf.default) can be customized by passing their name as long form arguments within your docker run command in the WAVEFRONT_PROXY_ARGS environment variable. For example, add `-e WAVEFRONT_PROXY_ARGS="--pushRateLimit 1000"` to your docker run command to specify a [rate limit](https://github.com/wavefrontHQ/java/blob/master/pkg/etc/wavefront/wavefront-proxy/wavefront.conf.default#L62) of 1000 pps for the proxy.

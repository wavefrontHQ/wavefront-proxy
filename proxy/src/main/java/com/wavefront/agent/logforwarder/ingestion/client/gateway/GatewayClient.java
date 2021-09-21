/*
 * Copyright (c) 2016 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.wavefront.agent.logforwarder.ingestion.client.gateway;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.GatewayConstants;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.IngestionGatewayUris;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.filter.GatewayClientFilter;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.metrics.MetricsManager;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.GatewayRequest;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.GatewayResponse;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.utils.Utils;
import com.wavefront.agent.logforwarder.ingestion.http.client.HttpClientFactory;
import com.wavefront.agent.logforwarder.ingestion.util.ClientUtils;
import com.wavefront.agent.logforwarder.ingestion.util.UriUtils;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.exception.GatewayException;
import io.vertx.ext.web.client.WebClient;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.dropwizard.metrics5.Meter;
import io.dropwizard.metrics5.MetricRegistry;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

/**
 * This class provides the interface for sending stream and route request to LeMans gateway.
 * Generate a @{@link GatewayRequest} with the URL starting with @{@link com.wavefront.agent.logforwarder
 * .constants.LogForwarderUris#STREAM   S}
 * for stream requests anything else will be considered a Route request.
 */

public class GatewayClient {

    public static final String METRICS_GATEWAY_CLIENT = MetricsManager.METRICS_CLIENT_PREFIX + "gateway_service.";
    public static final String PROPERTY_NAME_DISABLE_HTTP2 =
            GatewayConstants.PROPERTY_NAME_PREFIX + "gatewayClient.disableHttp2";
    public static final String METRICS_GATEWAY_CLIENT_SUCCESS_METRIC = METRICS_GATEWAY_CLIENT + MetricsManager.SUCCESS;
    public static final String METRICS_GATEWAY_CLIENT_FAILURE_METRIC = METRICS_GATEWAY_CLIENT + MetricsManager.FAILED;
    public static final String METRICS_GATEWAY_CLIENT_TOTAL_METRIC = METRICS_GATEWAY_CLIENT + MetricsManager.TOTAL;
    private static final Logger logger = LoggerFactory.getLogger(GatewayClient.class);
    public static final long DEFAULT_OPERATION_TIMEOUT_MICROS = TimeUnit.SECONDS.toMicros(60L);
    public static final String PROPERTY_URI_CACHE_SIZE =
            GatewayConstants.PROPERTY_NAME_PREFIX + "gatewayClient.uriCacheSize";
    public static final long DEFAULT_URI_CACHE_SIZE = 10000;

    private final URI lemansServiceUri;
    private final String accessKeyToken;
    protected final boolean disableHttp2;
    private final MetricRegistry metricRegistry;
    protected final LoadingCache<URI, URI> uriCache;
    private final List<GatewayClientFilter> gatewayClientFilters;
    private long operationTimeoutMicros;
    private final Meter successMeter;
    private final Meter failureMeter;
    private final Meter totalMeter;
    private final Vertx vertx;
    private final WebClient webClient;

    /**
     *
     * @param LogIngestionServiceUri public endpoint for ingesting logs
     * @param accessKey ingestion key
     * @param metricRegistry
     * @param vertx
     */
    public GatewayClient(URI LogIngestionServiceUri, String accessKey, MetricRegistry metricRegistry,
                         Vertx vertx) {
        this.lemansServiceUri = LogIngestionServiceUri;
        this.accessKeyToken = (accessKey != null) ? GatewayConstants.BEARER + ' ' + accessKey : null;
        this.metricRegistry = metricRegistry;
        this.gatewayClientFilters = new ArrayList<>();
        this.disableHttp2 = Boolean.parseBoolean(System.getProperty(PROPERTY_NAME_DISABLE_HTTP2, String.valueOf(true)));
        this.operationTimeoutMicros = Long.getLong(
                GatewayConstants.PROPERTY_NAME_CLIENT_OPERATION_TIMEOUT_MICROS,
                DEFAULT_OPERATION_TIMEOUT_MICROS);
        this.uriCache = CacheBuilder.newBuilder()
                .maximumSize(Long.getLong(PROPERTY_URI_CACHE_SIZE, DEFAULT_URI_CACHE_SIZE))
                .build(new CacheLoader<URI, URI>() {
                    @Override
                    public URI load(URI uri) {
                        return createNewUri(uri);
                    }
                });
        this.totalMeter = this.metricRegistry.meter(METRICS_GATEWAY_CLIENT_TOTAL_METRIC);
        this.successMeter = this.metricRegistry.meter(METRICS_GATEWAY_CLIENT_SUCCESS_METRIC);
        this.failureMeter = this.metricRegistry.meter(METRICS_GATEWAY_CLIENT_FAILURE_METRIC);
        this.vertx = vertx;
        HttpClientFactory.Builder builder = new HttpClientFactory.Builder()
                .vertx(vertx)
                .useNetworkProxy(true)
                .maxPoolSize(Integer.getInteger(ClientConstants.VERTX_GATEWAY_CLIENT_CONNECTIONS_PER_HOST, 128))
                .ssl(true);
        this.webClient = HttpClientFactory.createWebClient(builder);
    }

    public CompletableFuture<GatewayResponse> sendRequest(GatewayRequest request) {
        this.totalMeter.mark();
        CompletableFuture<GatewayResponse> responseFuture = new CompletableFuture<>();
        GatewayResponse gatewayResponse = GatewayResponse.createResponse(request.getAction(), request.getUri(),
                request.getId());

        // Apply the gateway filters, if we get a response use it or make a request to gateway
        Optional<GatewayResponse> filtersResponse;
        try {
            filtersResponse = applyGatewayClientFilters(request);
        } catch (Throwable th) {
            logger.error("Error while applying gateway client filters {}", Utils.toString(th));
            gatewayResponse.fail(th);
            responseFuture.completeExceptionally(new GatewayException(gatewayResponse,
                    "Error while applying gateway client filters", th));
            return responseFuture;
        }

        if (filtersResponse.isPresent()) {
            GatewayResponse response = filtersResponse.get();
            gatewayResponse
                    .setStatusCode(response.getStatusCode())
                    .setContentType(response.getContentType())
                    .setBodyNoCloning(response.getBody())
                    .addHeaders(response.getHeaders());
            responseFuture.complete(gatewayResponse);
            return responseFuture;
        }

        // Proceed with making call to gateway
        makeGatewayRequest(request, responseFuture, gatewayResponse);
        return responseFuture;
    }

    void addGatewayClientFilter(GatewayClientFilter gatewayClientFilter) {
        this.gatewayClientFilters.add(gatewayClientFilter);
    }

    private void makeGatewayRequest(GatewayRequest request, CompletableFuture<GatewayResponse> responseFuture,
                               GatewayResponse gatewayResponse) {

        // in case the auth token is null we cannot
        // forward requests to the lemans service, this
        // can happen when auth-token renewal failed.
        Pair<String, String> authHeader = getAuthHeader();
        if (authHeader == null) {
            gatewayResponse.fail(GatewayConstants.STATUS_CODE_UNAVAILABLE);
            responseFuture.complete(gatewayResponse);
            this.failureMeter.mark();
            return;
        }

        Pair<byte[], String> encodedBody;
        try {
            encodedBody = ClientUtils.encode(request.getBody(), request.getContentType(),
                    request.getAction().toString(),
                request.getHeader(GatewayConstants.HEADER_NAME_CONTENT_ENCODING));
        } catch (Exception ex) {
            logger.error("Unable to convert request body to buffer {}", Utils.toString(ex));
            gatewayResponse.fail(ex, GatewayConstants.STATUS_CODE_BAD_REQUEST);
            responseFuture.complete(gatewayResponse);
            this.failureMeter.mark();
            return;
        }

        URI outboundUri = this.uriCache.getUnchecked(request.getUri());

        Handler<AsyncResult<HttpResponse<Buffer>>> handler = res -> {
            if (res.failed()) {
                this.failureMeter.mark();
                logger.error("Error forwarding requests to gateway {}", Utils.toString(res.cause()));
                gatewayResponse.fail(res.cause());
            } else {
                HttpResponse<Buffer> result = res.result();
                String contentType = result.headers().get(HttpHeaders.CONTENT_TYPE) != null ?
                        result.headers().get(HttpHeaders.CONTENT_TYPE) : GatewayConstants.
                    MEDIA_TYPE_APPLICATION_JSON;

                gatewayResponse
                        .setStatusCode(result.statusCode())
                        .setContentType(contentType)
                        .setBodyNoCloning(ClientUtils.decode(result.body(), contentType))
                        .addHeaders(ClientUtils.getRequestHeaders(result.headers()));

                if (result.statusCode() < HttpResponseStatus.BAD_REQUEST.code()) {
                    this.successMeter.mark();
                } else {
                    this.failureMeter.mark();
                    logger.error("Error forwarding requests to gateway status code {}, resp: {}",
                            result.statusCode(), result.bodyAsString());
                    gatewayResponse.fail(new RuntimeException(result.bodyAsString()));
                }
            }
            // Process any GatewayResponseHandlers
            processGatewayResponseHandlers(request, gatewayResponse, responseFuture);
        };

        // Honour any timeout specified on request otherwise use default
        long timeoutMicros = request.getTimeoutMicros() > 0 ? request.getTimeoutMicros() : this.operationTimeoutMicros;

        HttpMethod httpMethod = ClientUtils.httpMethod(request.getAction().name());
        HttpRequest<Buffer> httpRequest = webClient.requestAbs(httpMethod, outboundUri.toString())
                .putHeader(authHeader.getKey(), authHeader.getValue())
                .putHeader(GatewayConstants.CONTENT_TYPE_HEADER, request.getContentType())
                .timeout(TimeUnit.MICROSECONDS.toMillis(timeoutMicros))
                .putHeaders(ClientUtils.convertToMultiMap(request.getHeaders()));

        if (encodedBody.getRight() != null) {
            httpRequest.putHeader(HttpHeaders.CONTENT_TYPE.toString(), encodedBody.getRight());
        }
        httpRequest.sendBuffer(Buffer.buffer(encodedBody.getLeft()), handler);
    }

    private void processGatewayResponseHandlers(GatewayRequest gatewayRequest, GatewayResponse gatewayResponse,
                                                CompletableFuture<GatewayResponse> responseFuture) {
        if (!gatewayRequest.getGatewayResponseHandlers().isEmpty()) {
            Iterator<GatewayResponse.GatewayResponseHandler> iterator = gatewayRequest.getGatewayResponseHandlers().iterator();
            while (iterator.hasNext()) {
                GatewayResponse.GatewayResponseHandler responseHandler = iterator.next();
                responseHandler.handle(gatewayResponse, gatewayResponse.getFailure());
            }
        }

        if (gatewayResponse.getFailure() != null) {
            responseFuture.completeExceptionally(new GatewayException(gatewayResponse,
                    "Error forwarding requests to gateway", gatewayResponse.getFailure()));
        } else {
            responseFuture.complete(gatewayResponse);
        }
    }

    private Optional<GatewayResponse> applyGatewayClientFilters(GatewayRequest gatewayRequest) {

        if (!isStreamRequest(gatewayRequest) || this.gatewayClientFilters.size() == 0) {
            return Optional.empty();
        }

        logger.debug("Applying gateway client filters");
        // Apply the filters in the same order they are present in the list
        Iterator<GatewayClientFilter> filterIterator = this.gatewayClientFilters.iterator();
        GatewayClientFilter filter = filterIterator.next();
        return applyGatewayClientFilter(filter, gatewayRequest, filterIterator);
    }

    private Optional<GatewayResponse> applyGatewayClientFilter(GatewayClientFilter filter, GatewayRequest gatewayRequest,
                                                               Iterator<GatewayClientFilter> filterIterator) {

        Optional<GatewayResponse> filterResponse = filter.applyFilter(gatewayRequest);
        if (filterResponse.isPresent()) {
            return filterResponse;
        }

        // Proceed to next filter
        if (filterIterator.hasNext()) {
            GatewayClientFilter nextFilter = filterIterator.next();
            return applyGatewayClientFilter(nextFilter, gatewayRequest, filterIterator);
        }
        return Optional.empty();
    }

    private URI createNewUri(URI requestUri) {
        try {
            String path = requestUri.getPath();
            if (path.startsWith(IngestionGatewayUris.STREAMS)) {
                path = UriUtils.buildUriPath(IngestionGatewayUris.LEMANS_V1_PREFIX, path);
            } else {
                path = UriUtils.buildUriPath(IngestionGatewayUris.ROUTERS_PREFIX, path);
            }
            return new URI(
                    this.lemansServiceUri.getScheme(), requestUri.getUserInfo(), this.lemansServiceUri.getHost(),
                    this.lemansServiceUri.getPort(), path, requestUri.getQuery(), requestUri.getFragment());
        } catch (URISyntaxException x) {
            throw new IllegalArgumentException(x.getMessage(), x);
        }
    }

    /**
     * For wavefont proxy use static access token for ingestion
     * @return
     */
    private Pair<String, String> getAuthHeader() {
        //TODO Remove before merge
//        // Use token provider if available otherwise use accessKey
//        if (this.tokenProvider != null) {
//            String authToken = this.tokenProvider.getToken();
//            if (authToken != null) {
//                return Pair.of(GatewayContants.REQUEST_AUTH_TOKEN_HEADER, authToken);
//            }
//            return null;
//        }

        if (this.accessKeyToken != null) {
            return Pair.of(GatewayConstants.AUTHORIZATION_HEADER, this.accessKeyToken);
        }

        return null;
    }

    private boolean isStreamRequest(GatewayRequest request) {
        return request.getUri().getPath().startsWith(IngestionGatewayUris.STREAMS);
    }
}

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

package com.wavefront.agent.logforwarder.ingestion.client.gateway.filter;

import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.GatewayRequest;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.GatewayResponse;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.utils.StreamUtils;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.dropwizard.metrics5.MetricRegistry;

public class RateLimitFilter implements GatewayClientFilter {
    private static final Logger logger = LoggerFactory.getLogger(RateLimitFilter.class.getName());
    private final MetricRegistry metricRegistry;
    private final String agentId;
    private final long retryIntervalSeconds;
    private Map<String, RateLimitState> streamToRateLimitMapping = new ConcurrentHashMap();

    public RateLimitFilter(MetricRegistry metricRegistry, String agentId) {
        this.metricRegistry = metricRegistry;
        this.agentId = agentId;
        this.retryIntervalSeconds = Long.getLong("lemans.rateLimitRetryIntervalSeconds", 600L);
    }

    public Optional<GatewayResponse> applyFilter(GatewayRequest request) {
        CompletableFuture<GatewayResponse> responseFuture = new CompletableFuture();
        GatewayResponse response = GatewayResponse.createResponse(request.getAction(), request.getUri(), request.getId());
        String streamName = StreamUtils.getStreamName(request.getUri(), "/");
        if (this.streamToRateLimitMapping.containsKey(streamName)) {
            RateLimitState currentStreamRateLimitState = (RateLimitState)this.streamToRateLimitMapping.get(streamName);
            if (currentStreamRateLimitState.retryAfterRateLimitIntervalMicros > Utils.getSystemNowMicrosUtc()) {
                this.incrementMetric(this.agentId, streamName);
                response.fail(429);
                responseFuture.complete(response);
                return Optional.of(response);
            }

            this.streamToRateLimitMapping.remove(streamName);
            logger.info("Rate Limit become Inactive {} at {}", streamName, Utils.getSystemNowMicrosUtc());
        }

        request.nestGatewayResponseHandler((resp, th) -> {
            if (resp.getStatusCode() == 429 && resp.getHeader("Retry-After") != null) {
                RateLimitState updatedRateLimitState = new RateLimitState();
                updatedRateLimitState.rateLimitIsActiveTillMicros = Long.parseLong(resp.getHeader("Retry-After"));
                if (Utils.fromNowMicrosUtc(TimeUnit.SECONDS.toMicros(this.retryIntervalSeconds)) > updatedRateLimitState.
                    rateLimitIsActiveTillMicros) {
                    updatedRateLimitState.retryAfterRateLimitIntervalMicros = updatedRateLimitState.rateLimitIsActiveTillMicros;
                } else {
                    updatedRateLimitState.retryAfterRateLimitIntervalMicros = Utils.fromNowMicrosUtc(TimeUnit.
                        SECONDS.toMicros(this.retryIntervalSeconds));
                }

                this.streamToRateLimitMapping.put(streamName, updatedRateLimitState);
                logger.info("Rate Limit has been active for given Agent Id: {} and Stream: {} till {}: ",
                    new Object[]{this.agentId, streamName, updatedRateLimitState.rateLimitIsActiveTillMicros});
            }

        });
        return Optional.empty();
    }

    private void incrementMetric(String agentId, String streamName) {
        String metricName;
        if (this.agentId != null) {
            metricName = String.format("lemans.client.ratelimiter.blobs.dropped.[stream:%s][agent:%s]", streamName, agentId);
            this.metricRegistry.meter(metricName).mark();
        } else {
            metricName = String.format("lemans.client.ratelimiter.blobs.dropped.[stream:%s]", streamName);
            this.metricRegistry.meter(metricName).mark();
        }

    }
}

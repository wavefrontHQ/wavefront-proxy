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

import com.wavefront.agent.logforwarder.ingestion.client.gateway.GatewayClient;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.buffer.disk.GsonConverter;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.GatewayConstants;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.metrics.ServiceStats;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.GatewayClientRequest;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.GatewayOperation;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.GatewayRequest;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.GatewayResponse;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.exception.GatewayException;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.buffer.disk.DiskbackedQueue;

import io.dropwizard.metrics5.Meter;
import io.dropwizard.metrics5.MetricName;
import io.dropwizard.metrics5.MetricRegistry;

import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.BACKPRESSURE_ACTIVE_METRIC;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.BACKPRESSURE_DEFAULT_DISPATCH_NUM_REQUESTS_BASE;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.BACKPRESSURE_DEFAULT_DISPATCH_NUM_REQUESTS_MAX_EXPONENT;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.BACKPRESSURE_DEFAULT_FAILURES_THRESHOLD_PER_SECOND;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.BACKPRESSURE_DEFAULT_MAINTENANCE_INTERVAL_MILLIS;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.BACKPRESSURE_DEFAULT_MAX_REQUEST_IN_QUEUE_DURATION_MICROS;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.BACKPRESSURE_DEFAULT_MAX_REQUEST_RETRY_COUNT;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.BACKPRESSURE_EXPONENT_METRIC;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.BACKPRESSURE_NON_RETRIABLE_FAILURE_OPERATION_METER;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.BACKPRESSURE_OPERATIONS_DROPPED_METER;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.BACKPRESSURE_OPERATIONS_QUEUED_METER;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.BACKPRESSURE_QUEUE_SIZE_METRIC;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.BACKPRESSURE_RETRIABLE_FAILURE_OPERATION_METER;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.PRAGMA_DIRECTIVE_BACKPRESSURE_DISPATCH;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.PROPERTY_NAME_BACKPRESSURE_DISPATCH_NUM_REQUESTS_BASE;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.PROPERTY_NAME_BACKPRESSURE_DISPATCH_NUM_REQUESTS_MAX_EXPONENT;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.PROPERTY_NAME_BACKPRESSURE_FAILURES_THRESHOLD_PER_SECOND;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.PROPERTY_NAME_BACKPRESSURE_MAINTENANCE_INTERVAL_MILLIS;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.PROPERTY_NAME_BACKPRESSURE_MAX_REQUEST_IN_QUEUE_DURATION_MICROS;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.ClientConstants.PROPERTY_NAME_BACKPRESSURE_MAX_REQUEST_RETRY_COUNT;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.GatewayConstants.STATUS_CODE_ACCEPTED;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.GatewayConstants.STATUS_CODE_TIMEOUT;
import static com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.GatewayConstants.STATUS_CODE_UNAVAILABLE;

/**
 * GatewayClientFilter to support buffering of requests to the lemans gateway in the event of request failures
 * due to service unavailability.
 * Main responsibilities include:
 * Detecting when backpressure needs to be activated and de-activated.
 * Buffering incoming requests on disk.
 * Draining buffered requests by forwarding them to the Le-Mans Cloud Service.
 */
public class BackPressureFilter implements GatewayClientFilter {
    private static final Logger logger = LoggerFactory.getLogger(BackPressureFilter.class);
    private static final int DEFAULT_SCHEDULER_THREAD_POOL_SIZE = 1;
    private final int failuresThresholdPerSecond;
    private final long maintenanceIntervalMillis;
    private final int dispatchNumRequestsBase;
    private final int dispatchNumRequestsMaxExponent;
    private final int maxRequestRetryCount;
    private final long maxRequestInQueueDurationMicros;
    private final long operationTimeoutMicros;

    private final GatewayClient gatewayClient;
    private final MetricRegistry metricRegistry;

    /* State */
    private final AtomicBoolean active = new AtomicBoolean(false);
    private final AtomicBoolean processingQueuedRequests = new AtomicBoolean(false);
    private final ServiceStats.TimeSeriesStats timeSeriesStats;
    private final AtomicInteger exponent = new AtomicInteger(0);
    private final DiskbackedQueue<GatewayClientRequest> requestQueue;
    private final String backpressureQueueLocation;
    private final ScheduledExecutorService scheduledExecutorService;

    private final Meter retriableFailureOperationMeter;
    private final Meter nonRetriableFailureOperationMeter;
    private final Meter requestsQueuedMeter;
    private final Meter requestsDroppedMeter;

    public BackPressureFilter(GatewayClient gatewayClient,
                              String backpressureQueueLocation, MetricRegistry metricRegistry) {
        this.gatewayClient = gatewayClient;
        this.backpressureQueueLocation = backpressureQueueLocation;
        this.metricRegistry = metricRegistry;

        this.failuresThresholdPerSecond = Integer.getInteger(
                PROPERTY_NAME_BACKPRESSURE_FAILURES_THRESHOLD_PER_SECOND, BACKPRESSURE_DEFAULT_FAILURES_THRESHOLD_PER_SECOND);
        this.maintenanceIntervalMillis = Integer.getInteger(
                PROPERTY_NAME_BACKPRESSURE_MAINTENANCE_INTERVAL_MILLIS, BACKPRESSURE_DEFAULT_MAINTENANCE_INTERVAL_MILLIS);
        this.dispatchNumRequestsBase = Integer.getInteger(
                PROPERTY_NAME_BACKPRESSURE_DISPATCH_NUM_REQUESTS_BASE, BACKPRESSURE_DEFAULT_DISPATCH_NUM_REQUESTS_BASE);
        this.dispatchNumRequestsMaxExponent = Integer.getInteger(
                PROPERTY_NAME_BACKPRESSURE_DISPATCH_NUM_REQUESTS_MAX_EXPONENT, BACKPRESSURE_DEFAULT_DISPATCH_NUM_REQUESTS_MAX_EXPONENT);
        this.maxRequestRetryCount = Integer.getInteger(PROPERTY_NAME_BACKPRESSURE_MAX_REQUEST_RETRY_COUNT,
                BACKPRESSURE_DEFAULT_MAX_REQUEST_RETRY_COUNT);
        this.timeSeriesStats = new ServiceStats.TimeSeriesStats(
                60, TimeUnit.SECONDS.toMillis(1), EnumSet.of(com.wavefront.agent.logforwarder.ingestion.client.gateway.metrics.ServiceStats.TimeSeriesStats.AggregationType.SUM));
        this.maxRequestInQueueDurationMicros =
                Long.getLong(PROPERTY_NAME_BACKPRESSURE_MAX_REQUEST_IN_QUEUE_DURATION_MICROS,
                        BACKPRESSURE_DEFAULT_MAX_REQUEST_IN_QUEUE_DURATION_MICROS);
        this.operationTimeoutMicros = Long.getLong(
                GatewayConstants.PROPERTY_NAME_CLIENT_OPERATION_TIMEOUT_MICROS,
                GatewayClient.DEFAULT_OPERATION_TIMEOUT_MICROS);


        // Initialize disk-backed queue
        if (this.backpressureQueueLocation == null || this.backpressureQueueLocation.isEmpty()) {
            String message = "backpressureQueueLocation cannot be empty! Cannot initialize BackpressureService";
            logger.error(message);
            throw new IllegalArgumentException(message);
        }

        File requestFolder = new File(this.backpressureQueueLocation);
        if (!requestFolder.exists()) {
            String message = String.format("Folder %s does not exist! Cannot initialize BackpressureService",
                    this.backpressureQueueLocation);
            logger.error(message);
            throw new IllegalArgumentException(message);
        }

        try {
            this.requestQueue = new DiskbackedQueue(requestFolder,
                new GsonConverter(GatewayClientRequest.class) {
                });
            logger.info("Request queue location: {}", requestFolder.getAbsolutePath());
        } catch (IOException e) {
            String message =  "Failed to initialize DiskbackedQueue. Failed to start BackpressureService.";
            logger.error(message, e);
            throw new IllegalStateException(message, e);
        }

        //Track metrics
        this.metricRegistry.gauge(new MetricName(BACKPRESSURE_ACTIVE_METRIC, new HashMap<>()),
            () -> this.active::get);
        this.metricRegistry.gauge(new MetricName(BACKPRESSURE_EXPONENT_METRIC,
         new HashMap<>()),   () -> this.exponent::get);

        // NOTE: Dropwizard docs recommend not using Gauges for tracking queue size since .size() operations are O(n) - meaning
        // the gauge will be slow. However, this is the most straight-forward way to track this metric right now.
        this.metricRegistry.gauge(new MetricName(BACKPRESSURE_QUEUE_SIZE_METRIC, new HashMap<>())
            , () -> this::getQueueSize);

        this.retriableFailureOperationMeter = this.metricRegistry.meter(BACKPRESSURE_RETRIABLE_FAILURE_OPERATION_METER);
        this.nonRetriableFailureOperationMeter = this.metricRegistry.meter(BACKPRESSURE_NON_RETRIABLE_FAILURE_OPERATION_METER);
        this.requestsQueuedMeter = this.metricRegistry.meter(BACKPRESSURE_OPERATIONS_QUEUED_METER);
        this.requestsDroppedMeter = this.metricRegistry.meter(BACKPRESSURE_OPERATIONS_DROPPED_METER);

        // Log settings
        logSettings();

        // Create and
        this.scheduledExecutorService =
                Executors.newScheduledThreadPool(DEFAULT_SCHEDULER_THREAD_POOL_SIZE,
                        r -> new Thread(r, getClass().getSimpleName() + "-scheduled"));
        this.scheduledExecutorService.scheduleAtFixedRate( this::processQueuedRequests,
                this.maintenanceIntervalMillis, this.maintenanceIntervalMillis,
                TimeUnit.MICROSECONDS);
    }

    private void processQueuedRequests() {
        /* This method is primarily used for retrying queued requests. Retries happen in an exponential
         manner, by trying to dispatch operations in batches. The batch size keeps growing exponentially as requests
         succeed. If failures occur, the dispatch mechanism resets itself, to start from the first batch size again.*/

        if (this.processingQueuedRequests.get()) {
            return;
        }

        this.processingQueuedRequests.set(true);

        try {
            this.requestQueue.flushToDisk();
        } catch (IOException e) {
            logger.error("Error while writing to the queue file: {}", Utils.toString(e));
            this.processingQueuedRequests.set(false);
            return;
        }

        /* If there are no queued requests, that means all failed requests were retried and they succeeded.
        In this case, let's check and de-activate backpressure. */
        if (this.requestQueue.isEmpty()) {
            // Deactivate backpressure the queue is empty
            if (this.active.compareAndSet(true, false)) {
                logger.info("Deactivating the backpressure. All queued requests were successful!");
            }
            this.processingQueuedRequests.set(false);
            return;
        }

        // If we do have queued requests, we start dispatching them in batches.
        try {
            dispatchRequests((int) Math.pow(this.dispatchNumRequestsBase, this.exponent.get()))
                    .thenAccept(success -> {
                        if (!success) {
                            // if any requests failed during dispatch then reset the exponent to start again
                            this.exponent.set(0);
                        } else {
                            if (this.exponent.get() < this.dispatchNumRequestsMaxExponent) {
                                // move to the next stage
                                this.exponent.incrementAndGet();
                            } else {
                                // if all stages pass then deactivate the backpressure
                                if (this.active.compareAndSet(true, false)) {
                                    logger.info("Deactivating the backpressure. Queue size is {}",
                                            this.requestQueue.size());
                                }
                            }
                        }
                        this.processingQueuedRequests.set(false);
                    })
                    .exceptionally(ex -> {
                        this.processingQueuedRequests.set(false);
                        logger.error("Error dispatching requests from the queue {}", Utils.toString(ex));
                        return null;
                    });
        } catch (Throwable e) {
            this.processingQueuedRequests.set(false);
            logger.error("Error dispatching requests from the queue {}", Utils.toString(e));
        }
    }

    @SuppressWarnings("rawtypes")
    private CompletableFuture<Boolean> dispatchRequests(int batchSize) {
        /* This method tries to dispatch one batch of queued requests. If the dispatched requests all succeed,
         the method removes these operations from the queue. If any of the operations fail
         with a retryable error, this method keeps the operations queued. */

        CompletableFuture<Boolean> successResult = new CompletableFuture<>();
        List<GatewayRequest> gatewayRequestList = new ArrayList<>();
        Map<Long, GatewayClientRequest> requestMap = new HashMap<>();

        List<GatewayClientRequest> requestList;
        int numRequestsRemoved;

        try {
            requestList = this.requestQueue.peek(batchSize);
            numRequestsRemoved = requestList.size();
        } catch (Throwable e) {
            logger.error("Error occurred while reading elements from the queue file: {}", Utils.toString(e));
            successResult.completeExceptionally(e);
            return successResult;
        }

        if (requestList.isEmpty()) {
            successResult.complete(true);
            return successResult;
        }

        requestList.forEach(request -> {
            GatewayRequest gatewayRequest = buildGatewayRequest(request);
            gatewayRequestList.add(gatewayRequest);
            requestMap.put(gatewayRequest.getId(), request);
        });

        CompletableFuture<Collection<GatewayClientRequest>> requestResult = new CompletableFuture<>();
        requestResult.thenAccept(requests -> {
            try {
                for (GatewayClientRequest r : requests) {
                    // Increment retry count
                    r.retryCount++;

                    if (hasExceededRetryAttempts(r)) {
                        this.requestsDroppedMeter.mark();
                        logger.error(
                                "Dropping a request because the retry attempts have been exceeded. " +
                                        "Total dropped requests: {}", this.requestsDroppedMeter.getCount());
                    } else if (!this.requestQueue.add(r)) {
                        /* If we cannot add requests back to the queue because the queue is full
                         then these requests will be dropped. */
                        this.requestsDroppedMeter.mark();
                        logger.error("Dropping a request because the queue is full. Total dropped requests: {}",
                                this.requestsDroppedMeter.getCount());
                    }
                }

                this.requestQueue.remove(numRequestsRemoved);
                successResult.complete(requests.isEmpty());
            } catch (IOException e) {
                logger.error("Error occurred while writing/removing elements to the file: {}", Utils.toString(e));
                successResult.completeExceptionally(e);
            }
        });

        logger.info("Dispatching {} requests to gateway client", gatewayRequestList.size());

        List<CompletableFuture<GatewayResponse>> responseFutures = new ArrayList<>();
        gatewayRequestList.forEach(gatewayRequest -> {
            responseFutures.add(this.gatewayClient.sendRequest(gatewayRequest)
                    .whenComplete(((resp, th) -> {
                        GatewayResponse gatewayResponse = resp;
                        if (th != null) {
                            GatewayException ge = (GatewayException)th;
                            gatewayResponse = ge.getGatewayResponse();
                        }
                        if (!shouldRetry(gatewayResponse)) {
                            requestMap.remove(gatewayResponse.getId());
                        }
                    })));
        });

        CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[responseFutures.size()]))
                .whenComplete((aVoid, throwable) -> {
                    requestResult.complete(requestMap.values());
                });

        return successResult;
    }

    private boolean hasExceededRetryAttempts(GatewayClientRequest request) {
        return request.retryCount > this.maxRequestRetryCount &&
                (Utils.getSystemNowMicrosUtc() - request.receivedTimeMicros) >
                        this.maxRequestInQueueDurationMicros;
    }

    private GatewayRequest buildGatewayRequest(GatewayClientRequest request) {
        GatewayRequest gatewayRequest = GatewayRequest.createRequest(GatewayOperation.Action.valueOf(request.action), request.uri);
        if (request.headers != null) {
            gatewayRequest.addHeaders(request.headers);
        }
        gatewayRequest.setBody(request.bodyBytes != null ? request.bodyBytes : request.body);
        gatewayRequest.setContentType(request.contentType);
        gatewayRequest.addHeader(PRAGMA_DIRECTIVE_BACKPRESSURE_DISPATCH,
                PRAGMA_DIRECTIVE_BACKPRESSURE_DISPATCH);
        return gatewayRequest;
    }

    public void stop() {
        if (this.scheduledExecutorService != null) {
            this.scheduledExecutorService.shutdown();
        }
    }

    public BackPressureStatus getStatus() {
        BackPressureStatus backPressureStatus = new BackPressureStatus();
        backPressureStatus.isActive = this.active.get();
        backPressureStatus.queueSize = this.getQueueSize();
        backPressureStatus.numDroppedRequests = (int)this.requestsDroppedMeter.getCount();
        return backPressureStatus;
    }

    private int getQueueSize() {
        return this.requestQueue.size() + this.requestQueue.bufferSize();
    }

    private void logSettings() {
        logger.info("failuresThresholdPerSecond = {}", this.failuresThresholdPerSecond);
        logger.info("maintenanceIntervalMillis = {}", this.maintenanceIntervalMillis);
        logger.info("dispatchNumRequestsBase = {}", this.dispatchNumRequestsBase);
        logger.info("dispatchNumRequestsMaxExponent = {}", this.dispatchNumRequestsMaxExponent);
    }

    public Optional<GatewayResponse> applyFilter(GatewayRequest request) {
        /* If this is a retry of a previously queued request by the backpressure implementation, then simply remove
         the backpressure tag and let it go through. */

        GatewayResponse response = GatewayResponse.createResponse(request.getAction(), request.getUri(),
                request.getId());
        if (request.getHeader(PRAGMA_DIRECTIVE_BACKPRESSURE_DISPATCH) != null) {
            request.removeHeader(PRAGMA_DIRECTIVE_BACKPRESSURE_DISPATCH);
            return Optional.empty();
        }

        GatewayClientRequest gatewayClientRequest = buildGatewayClientRequest(request);

        /* If this is a new request, let's see if backpressure is currently active. If so, we need to queue the
        request and complete the operation with Http status code - ACCEPTED. */
        if (this.active.get()) {
            if (queueInboundRequest(gatewayClientRequest)) {
                response.setStatusCode(GatewayConstants.STATUS_CODE_ACCEPTED);
            } else {
                logger.error("Could not queue failed request as queue file size limit has reached");
                response.fail(GatewayConstants.STATUS_CODE_UNAVAILABLE);
            }
            return Optional.of(response);
        }

        request.nestGatewayResponseHandler(((resp, failure) -> {
            /* If the request failed, the first thing we verify
             is whether the failure is eligible for retries */
            if (failure != null && shouldRetry(resp)) {
                this.retriableFailureOperationMeter.mark();

                /* Before queueing the request for retries, let's firstly look at the historical stats to
                determine if failures have exceeded the threshold limit. If so, we wil activate backpressure. */
                synchronized (this.timeSeriesStats) {
                    this.timeSeriesStats.add(Utils.getSystemNowMicrosUtc(), 0, 1);
                    Double latestBinValue = this.timeSeriesStats.bins
                            .get(this.timeSeriesStats.bins.lastKey()).sum;
                    if (!this.active.get() && latestBinValue > this.failuresThresholdPerSecond) {
                        this.active.set(true);
                        this.exponent.set(0);
                        logger.info("Backpressure is activated. Number of requests failed per second exceeds {}",
                                this.failuresThresholdPerSecond);
                    }
                }

                // Let's queue the failed request.
                if (queueInboundRequest(gatewayClientRequest)) {
                    resp.setBody(null);
                    resp.setStatusCode(STATUS_CODE_ACCEPTED);
                } else {
                    logger.error("Could not queue failed request as queue file size limit has reached");
                }
            } else if (failure != null) {
                this.nonRetriableFailureOperationMeter.mark();
            }
        }));

        return Optional.empty();
    }

    private boolean queueInboundRequest(GatewayClientRequest request) {
        this.requestsQueuedMeter.mark();
        try {
            return this.requestQueue.add(request);
        } catch (Throwable e) {
            logger.error("Cannot add request to the queue. {}", Utils.toString(e));
        }
        return false;
    }

    private static boolean shouldRetry(GatewayResponse response) {
        return (response.getStatusCode() == STATUS_CODE_TIMEOUT ||
                response.getStatusCode() == STATUS_CODE_UNAVAILABLE);
    }

    private static GatewayClientRequest buildGatewayClientRequest(GatewayRequest gatewayRequest) {
        GatewayClientRequest r = new GatewayClientRequest();
        r.uri = gatewayRequest.getUri();
        if (gatewayRequest.getBody() != null) {
            Object rawBody = gatewayRequest.getBody();
            if (rawBody instanceof byte[]) {
                r.bodyBytes = (byte[]) rawBody;
            } else {
                r.body = rawBody;
            }
        }
        r.contentType = gatewayRequest.getContentType();
        r.action = gatewayRequest.getAction().name();
        r.headers = gatewayRequest.getHeaders();
        r.receivedTimeMicros = Utils.getSystemNowMicrosUtc();
        return r;
    }
}

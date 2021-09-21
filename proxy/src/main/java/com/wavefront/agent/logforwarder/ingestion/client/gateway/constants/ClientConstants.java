package com.wavefront.agent.logforwarder.ingestion.client.gateway.constants;

import java.util.concurrent.TimeUnit;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 2:14 PM
 */
public interface ClientConstants {
  String METRICS_CLIENT_PREFIX = "lemans.client.";
  String AGENT_TAG = "[agent:%s]";
  String PROPERTY_NAME_BACKPRESSURE_FAILURES_THRESHOLD_PER_SECOND = "lemans.backpressure.failuresThresholdPerSecond";
  String PROPERTY_NAME_BACKPRESSURE_MAINTENANCE_INTERVAL_MILLIS = "lemans.backpressure.maintenanceIntervalMillis";
  String PROPERTY_NAME_BACKPRESSURE_DISPATCH_NUM_REQUESTS_BASE = "lemans.backpressure.dispatchNumRequestsBase";
  String PROPERTY_NAME_BACKPRESSURE_DISPATCH_NUM_REQUESTS_MAX_EXPONENT = "lemans.backpressure.dispatchNumRequestsMaxExponent";
  String PROPERTY_NAME_BACKPRESSURE_QUEUE_FOLDER = "lemans.backpressure.backpressureQueueFolder";
  String PROPERTY_NAME_BACKPRESSURE_MAX_REQUEST_RETRY_COUNT = "lemans.backpressure.maxRequestRetryCount";
  String PROPERTY_NAME_BACKPRESSURE_MAX_REQUEST_IN_QUEUE_DURATION_MICROS = "lemans.backpressure.requestInQueueDuration";
  String PROPERTY_NAME_RATE_LIMIT_INTERVAL_SECONDS = "lemans.rateLimitRetryIntervalSeconds";
  String PRAGMA_DIRECTIVE_BACKPRESSURE_DISPATCH = "lemans-backpressure";
  String BACKPRESSURE_ACTIVE_METRIC = "lemans.client.backpressure.active";
  String BACKPRESSURE_EXPONENT_METRIC = "lemans.client.backpressure.exponent";
  String BACKPRESSURE_QUEUE_SIZE_METRIC = "lemans.client.backpressure.queue.size";
  String BACKPRESSURE_RETRIABLE_FAILURE_OPERATION_METER = "lemans.client.backpressure.ops.failure.retriable";
  String BACKPRESSURE_NON_RETRIABLE_FAILURE_OPERATION_METER = "lemans.client.backpressure.ops.failure.non-retriable";
  String BACKPRESSURE_OPERATIONS_QUEUED_METER = "lemans.client.backpressure.ops.queued";
  String BACKPRESSURE_OPERATIONS_DROPPED_METER = "lemans.client.backpressure.ops.dropped";
  String SESSION_CLIENT_PREFIX = "lemans.client.sessionclient.";
  String SESSION_CLIENT_IS_CONNECTED_GAUGE = "lemans.client.sessionclient.is-connected";
  String SESSION_CLIENT_RECONNECT_ATTEMPT_GAUGE = "lemans.client.sessionclient.reconnect.attempt";
  String SESSION_CLIENT_HEARTBEAT_FAILURE_RECONNECT_ATTEMPT_METER = "lemans.client.sessionclient.reconnect.attempt.heartbeat-failure";
  String SESSION_CLIENT_SSE_EVENTS_RECEIVED_METER = "lemans.client.sessionclient.received.sse-events";
  String SESSION_CLIENT_PARTIAL_DATA_RECEIVED_METER = "lemans.client.sessionclient.received.partial-data";
  String SESSION_CLIENT_HTTP_CONNECTION_EXPIRY_METER = "lemans.client.sessionclient.http.connection.expiry";
  int BACKPRESSURE_DEFAULT_FAILURES_THRESHOLD_PER_SECOND = 50;
  int BACKPRESSURE_DEFAULT_MAINTENANCE_INTERVAL_MILLIS = 1000;
  int BACKPRESSURE_DEFAULT_DISPATCH_NUM_REQUESTS_BASE = 2;
  int BACKPRESSURE_DEFAULT_DISPATCH_NUM_REQUESTS_MAX_EXPONENT = 5;
  int BACKPRESSURE_DEFAULT_MAX_REQUEST_RETRY_COUNT = 10;
  int VERTX_HTTP_CLIENT_MAX_CHUNK_SIZE = 65536;
  long BACKPRESSURE_DEFAULT_MAX_REQUEST_IN_QUEUE_DURATION_MICROS = TimeUnit.MINUTES.toMicros(30L);
  long DEFAULT_RATE_LIMIT_INTERVAL_SECONDS = 600L;
  String STREAM_NAME_TAG = "[stream:%s]";
  String BLOBS_DROPPED_BY_CLIENT_WITH_ACTIVE_RATE_LIMIT = "lemans.client.ratelimiter.blobs.dropped.";
  String BLOBS_DROPPED_BY_CLIENT_WITH_ACTIVE_RATE_LIMIT_WITH_OUT_AGENT_ID = "lemans.client.ratelimiter.blobs.dropped.[stream:%s]";
  String BLOBS_DROPPED_BY_CLIENT_WITH_ACTIVE_RATE_LIMIT_WITH_AGENT_ID = "lemans.client.ratelimiter.blobs.dropped.[stream:%s][agent:%s]";
  String VERTX_HTTP_CLIENT_CONNECTIONS_PER_HOST = "vertx.http-client.connections-per-host";
  String VERTX_GATEWAY_CLIENT_CONNECTIONS_PER_HOST = "vertx.gateway-client.connections-per-host";
}

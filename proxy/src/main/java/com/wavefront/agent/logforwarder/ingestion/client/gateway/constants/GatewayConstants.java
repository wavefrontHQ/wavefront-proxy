package com.wavefront.agent.logforwarder.ingestion.client.gateway.constants;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/16/21 3:04 PM
 */
public class GatewayConstants {
  public static final String COMMANDS_QUERY_PARAM_LIMIT = "limit";
  public static final String AGENT_ID_HEADER = "lm-agent-id";
  public static final String STREAM_ID_HEADER = "lm-stream-id";
  public static final String AVAILABILITY_ZONES_HEADER = "lm-availability-zones";
  public static final String TENANT_ID_HEADER = "lm-tenant-id";
  public static final String ORG_ID_HEADER = "lm-org-id";
  public static final String ACCESS_KEY_ID_HEADER = "lm-access-key-id";
  public static final String TIMESTAMP_HEADER = "lm-timestamp";
  public static final String CONTENT_LENGTH_HEADER = "lm-content-length";
  public static final String RETRY_COUNT_HEADER = "lm-retry-count";
  public static final String RETRY_FLAG_HEADER = "kafka-retry-flag";
  public static final String PARTITION_INDEX_HEADER = "lm-partition-index";
  public static final String TOPIC_NAME_HEADER = "lm-topic-name";
  public static final String XENON_FORWARDED_HEADER = "lm-xenon-forwarded";
  public static final String SOURCE_AUTH_INFO_HEADER = "lm-source-auth-info";
  public static final String PROPAGATE_RESPONSE_HEADER = "propagate-response-headers";
  public static final String CONFIG_HTTP_SERVER_PORT_VERTX = "http.server.port.vertx";
  public static final String CONFIG_WEBCLIENT_MAX_CONNECTION = "webclient.max.connection";
  public static final String CONFIG_HTTP_SERVER_SOURCE_VERTX = "http.server.source.vertx";
  public static final String CONFIG_AVAILABILITY_ZONE = "availability.zone";
  public static final String CONFIG_AUTH_TOKEN_EXPIRATION_MICROS = "authToken.expirationMicros";
  public static final String CONFIG_CSP_URI_VERTX = "csp.uri.vertx";
  public static final String CONFIG_RESOURCE_HOST_URI_VERTX = "resource.host.uri.vertx";
  public static final String CONFIG_KAFKA_CONFIG_FILE_VERTX = "kafka.config.file.vertx";
  public static final String CONFIG_PROMETHEUS_METRICS_URI = "prometheus.metrics.uri";
  public static final String CONFIG_ENABLE_ACCESS_LOGS = "logging.enableAccessLogs";
  public static final String CONFIG_HTTP_SERVER_INSTANCES = "http.server.instances";
  public static final String CONFIG_REGISTRATION_ALLOWED_CSP_PERMISSIONS = "registration.allowedCspPermissions";
  public static final String CONFIG_USE_STANDARD_AUTH_HEADER = "http.requests.useStandardAuthHeader";
  public static final String VERTX_WAVEFRONT_PROXY_PREFIX = "proxy://";
  public static final String URI_PATH_CHAR = "/";
  public static final String URI_WILDCARD_CHAR = "*";
  public static final String REQUEST_AUTH_TOKEN_HEADER = "x-xenon-auth-token";
  public static final String RETRY_IMMEDIATELY_HEADER = "retry-immediately";
  public static final String AUTHORIZATION_HEADER = "authorization";
  public static final int VERTX_HTTP_SERVER_DEFAULT_PORT = 8080;
  public static final int VERTX_WEBCLIENT_MAX_CONNECTION = 128;
  public static final String PROPERTY_NAME_VERTX_HTTP_SERVER_PORT = "lemans.vertx.http.port";
  public static final String PROPERTY_NAME_VERTX_WEBCLIENT_MAX_CONNECTION = "lemans.vertx.webclient.max.connection";
  public static final String PROPERTY_NAME_VERTX_HTTP_SERVER_INSTANCES = "vertx.http.server.instances";
  public static final String PROPERTY_NAME_REGISTRATION_ALLOWED_CSP_PERMISSIONS = "lemans.registration.allowedCspPermissions";
  public static final String PROPERTY_NAME_PREFIX = "lemans.";
  public static final String PROPERTY_NAME_VERTX_ENABLE_ACCESS_LOGS = "lemans.vertx.enableAccessLogs";
  public static final String RECEIVERS_PARTITION_HEADER = "lm-partition-id";
  public static final String S3_METADATA_HEADER = "lm-s3-metadata";
  public static final String RECEIVER_NAME_HEADER = "lm-receiver-name";
  public static final long SESSION_EXPIRATION_MICROS;
  public static final long SESSION_EXPIRATION_MILLIS;
  public static final String SESSION_NOTIFICATION_TRIGGER_WATCHER = "triggerCommandWatcher";
  public static final String SESSION_HEART_BEAT_EVENT = "heartBeatEvent";
  public static final String HEADER_NAME_TRANSFER_ENCODING = "Transfer-Encoding";
  public static final String HEADER_NAME_CONTENT_ENCODING = "content-encoding";
  public static final String TRANSFER_ENCODING_CHUNKED = "chunked";
  public static final String BEARER = "Bearer";
  public static final int BEARER_LENGTH;
  public static final String LEMANS_TELEMETRY_STREAM_NAME = "lemans-telemetry";
  public static final String CONTENT_TYPE_HEADER = "content-type";
  public static final int STATUS_CODE_FAILURE_THRESHOLD = 400;
  public static final String MEDIA_TYPE_APPLICATION_OCTET_STREAM = "application/octet-stream";
  public static final String MEDIA_TYPE_APPLICATION_JSON = "application/json";
  public static final String MEDIA_TYPE_APPLICATION_X_WWW_FORM_ENCODED = "application/x-www-form-urlencoded";
  public static final int STATUS_CODE_OK = 200;
  public static final int STATUS_CODE_ACCEPTED = 202;
  public static final int STATUS_CODE_BAD_REQUEST = 400;
  public static final int STATUS_CODE_FORBIDDEN = 403;
  public static final int STATUS_CODE_TIMEOUT = 408;
  public static final int STATUS_CODE_INTERNAL_ERROR = 500;
  public static final int STATUS_CODE_UNAVAILABLE = 503;
  public static final int LEMANS_TELEMETRY_INTERVAL_SECONDS = 30;
  public static final String LEMANS_BACKPRESSURE_QUEUE_DIRECTORY = "backpressure-queue";
  public static final String PROPERTY_NAME_CLIENT_OPERATION_TIMEOUT_MICROS = "lemans.gatewayClient.operationExpiryMicros";
  public static final String LEMANS_RATE_LIMIT_RETRY_HEADER = "Retry-After";
  public static final int RATE_LIMIT_ENABLED_RESPONSE_CODE = 429;
  public static final String HEADER_EXTRA_PATH = "lemans.extra-path";
  public static final String HEADER_EXTRA_QUERY = "lemans.extra-query";
  public static final List<String> LEMANS_KAFKA_HEADERS;
  public static final String HOST_TYPE_XENON = "Xenon";
  public static final int DEFAULT_THREAD_COUNT;
  public static final String STREAM_EXTRA_PATH = "requestExtraPath";
  public static final String STREAM_REQUEST_QUERY = "requestQuery";
  public static final String MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM = "application/kryo-octet-stream";
  public static final String GZIP = "gzip";
  public static final String UTF8 = "UTF-8";
  public static int MAX_BINARY_SERIALIZED_BODY_LIMIT = Integer.getInteger("serviceClient" +
      ".MAX_BINARY_SERIALIZED_BODY_LIMIT", 1048576);

  protected static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

  protected static final String JWT_TYPE = "JWT";

  protected static final char JWT_SEPARATOR = '.';

  private GatewayConstants() {
  }

  static {
    SESSION_EXPIRATION_MICROS = TimeUnit.HOURS.toMicros(1L);
    SESSION_EXPIRATION_MILLIS = TimeUnit.HOURS.toMillis(1L);
    BEARER_LENGTH = "Bearer".length() + 1;
    LEMANS_KAFKA_HEADERS = Collections.unmodifiableList(Arrays.asList("lemans.extra-path", "lemans.extra-query"));
    DEFAULT_THREAD_COUNT = Math.max(4, Runtime.getRuntime().availableProcessors());
  }
}

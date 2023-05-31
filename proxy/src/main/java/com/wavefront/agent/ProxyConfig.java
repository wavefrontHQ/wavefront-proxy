package com.wavefront.agent;

import static com.wavefront.agent.ProxyConfig.ProxyAuthMethod.*;
import static com.wavefront.agent.config.ReportableConfig.reportGauge;
import static com.wavefront.agent.data.EntityProperties.*;
import static com.wavefront.common.Utils.getBuildVersion;
import static com.wavefront.common.Utils.getLocalHostName;
import static io.opentracing.tag.Tags.SPAN_KIND;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.auth.TokenValidationMethod;
import com.wavefront.agent.config.Categories;
import com.wavefront.agent.config.ProxyConfigOption;
import com.wavefront.agent.config.ReportableConfig;
import com.wavefront.agent.config.SubCategories;
import com.wavefront.agent.data.TaskQueueLevel;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.common.TimeProvider;
import com.yammer.metrics.core.MetricName;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Proxy configuration (refactored from {@link com.wavefront.agent.AbstractAgent}).
 *
 * @author vasily@wavefront.com
 */
@SuppressWarnings("CanBeFinal")
public class ProxyConfig extends ProxyConfigDef {
  static final int GRAPHITE_LISTENING_PORT = 2878;
  private static final Logger logger = Logger.getLogger(ProxyConfig.class.getCanonicalName());
  private static final double MAX_RETRY_BACKOFF_BASE_SECONDS = 60.0;
  private final List<Field> modifyByArgs = new ArrayList<>();
  private final List<Field> modifyByFile = new ArrayList<>();
  protected Map<String, TenantInfo> multicastingTenantList = Maps.newHashMap();

  TimeProvider timeProvider = System::currentTimeMillis;

  // Selecting the appropriate Wavefront proxy authentication method depending on the proxy
  // settings.
  public enum ProxyAuthMethod {
    CSP_API_TOKEN,
    WAVEFRONT_API_TOKEN,
    CSP_CLIENT_CREDENTIALS
  }

  public boolean isHelp() {
    return help;
  }

  public boolean isVersion() {
    return version;
  }

  public String getPrefix() {
    return prefix;
  }

  public boolean isTestLogs() {
    return testLogs;
  }

  public String getTestPreprocessorForPort() {
    return testPreprocessorForPort;
  }

  public String getTestSpanPreprocessorForPort() {
    return testSpanPreprocessorForPort;
  }

  public String getServer() {
    return server;
  }

  public String getBufferFile() {
    return bufferFile;
  }

  public int getBufferShardSize() {
    return bufferShardSize;
  }

  public boolean isDisableBufferSharding() {
    return disableBufferSharding;
  }

  public boolean isSqsQueueBuffer() {
    return sqsQueueBuffer;
  }

  public String getSqsQueueNameTemplate() {
    return sqsQueueNameTemplate;
  }

  public String getSqsQueueRegion() {
    return sqsQueueRegion;
  }

  public String getSqsQueueIdentifier() {
    return sqsQueueIdentifier;
  }

  public TaskQueueLevel getTaskQueueLevel() {
    return taskQueueLevel;
  }

  public String getExportQueuePorts() {
    return exportQueuePorts;
  }

  public String getExportQueueOutputFile() {
    return exportQueueOutputFile;
  }

  public boolean isExportQueueRetainData() {
    return exportQueueRetainData;
  }

  public boolean isUseNoopSender() {
    return useNoopSender;
  }

  public int getFlushThreads() {
    return flushThreads;
  }

  public int getFlushThreadsSourceTags() {
    return flushThreadsSourceTags;
  }

  public int getFlushThreadsEvents() {
    return flushThreadsEvents;
  }

  public int getFlushThreadsLogs() {
    return flushThreadsLogs;
  }

  public int getPushFlushIntervalLogs() {
    return pushFlushIntervalLogs;
  }

  public boolean isPurgeBuffer() {
    return purgeBuffer;
  }

  public int getPushFlushInterval() {
    return pushFlushInterval;
  }

  public int getPushFlushMaxPoints() {
    return pushFlushMaxPoints;
  }

  public int getPushFlushMaxHistograms() {
    return pushFlushMaxHistograms;
  }

  public int getPushFlushMaxSourceTags() {
    return pushFlushMaxSourceTags;
  }

  public int getPushFlushMaxSpans() {
    return pushFlushMaxSpans;
  }

  public int getPushFlushMaxSpanLogs() {
    return pushFlushMaxSpanLogs;
  }

  public int getPushFlushMaxEvents() {
    return pushFlushMaxEvents;
  }

  public int getPushFlushMaxLogs() {
    return pushFlushMaxLogs;
  }

  public double getPushRateLimit() {
    return pushRateLimit;
  }

  public double getPushRateLimitHistograms() {
    return pushRateLimitHistograms;
  }

  public double getPushRateLimitSourceTags() {
    return pushRateLimitSourceTags;
  }

  public double getPushRateLimitSpans() {
    return pushRateLimitSpans;
  }

  public double getPushRateLimitSpanLogs() {
    return pushRateLimitSpanLogs;
  }

  public double getPushRateLimitEvents() {
    return pushRateLimitEvents;
  }

  public double getPushRateLimitLogs() {
    return pushRateLimitLogs;
  }

  public int getPushRateLimitMaxBurstSeconds() {
    return pushRateLimitMaxBurstSeconds;
  }

  public int getPushMemoryBufferLimit() {
    return pushMemoryBufferLimit;
  }

  public int getPushMemoryBufferLimitLogs() {
    return pushMemoryBufferLimitLogs;
  }

  public int getPushBlockedSamples() {
    return pushBlockedSamples;
  }

  public String getBlockedPointsLoggerName() {
    return blockedPointsLoggerName;
  }

  public String getBlockedHistogramsLoggerName() {
    return blockedHistogramsLoggerName;
  }

  public String getBlockedSpansLoggerName() {
    return blockedSpansLoggerName;
  }

  public String getBlockedLogsLoggerName() {
    return blockedLogsLoggerName;
  }

  public String getPushListenerPorts() {
    return pushListenerPorts;
  }

  public int getPushListenerMaxReceivedLength() {
    return pushListenerMaxReceivedLength;
  }

  public int getPushListenerHttpBufferSize() {
    return pushListenerHttpBufferSize;
  }

  public int getTraceListenerMaxReceivedLength() {
    return traceListenerMaxReceivedLength;
  }

  public int getTraceListenerHttpBufferSize() {
    return traceListenerHttpBufferSize;
  }

  public int getListenerIdleConnectionTimeout() {
    return listenerIdleConnectionTimeout;
  }

  public int getMemGuardFlushThreshold() {
    return memGuardFlushThreshold;
  }

  public boolean isHistogramPassthroughRecompression() {
    return histogramPassthroughRecompression;
  }

  public String getHistogramStateDirectory() {
    return histogramStateDirectory;
  }

  public long getHistogramAccumulatorResolveInterval() {
    return histogramAccumulatorResolveInterval;
  }

  public long getHistogramAccumulatorFlushInterval() {
    return histogramAccumulatorFlushInterval;
  }

  public int getHistogramAccumulatorFlushMaxBatchSize() {
    return histogramAccumulatorFlushMaxBatchSize;
  }

  public int getHistogramMaxReceivedLength() {
    return histogramMaxReceivedLength;
  }

  public int getHistogramHttpBufferSize() {
    return histogramHttpBufferSize;
  }

  public String getHistogramMinuteListenerPorts() {
    return histogramMinuteListenerPorts;
  }

  public int getHistogramMinuteFlushSecs() {
    return histogramMinuteFlushSecs;
  }

  public short getHistogramMinuteCompression() {
    return histogramMinuteCompression;
  }

  public int getHistogramMinuteAvgKeyBytes() {
    return histogramMinuteAvgKeyBytes;
  }

  public int getHistogramMinuteAvgDigestBytes() {
    return histogramMinuteAvgDigestBytes;
  }

  public long getHistogramMinuteAccumulatorSize() {
    return histogramMinuteAccumulatorSize;
  }

  public boolean isHistogramMinuteAccumulatorPersisted() {
    return histogramMinuteAccumulatorPersisted;
  }

  public boolean isHistogramMinuteMemoryCache() {
    return histogramMinuteMemoryCache;
  }

  public String getHistogramHourListenerPorts() {
    return histogramHourListenerPorts;
  }

  public int getHistogramHourFlushSecs() {
    return histogramHourFlushSecs;
  }

  public short getHistogramHourCompression() {
    return histogramHourCompression;
  }

  public int getHistogramHourAvgKeyBytes() {
    return histogramHourAvgKeyBytes;
  }

  public int getHistogramHourAvgDigestBytes() {
    return histogramHourAvgDigestBytes;
  }

  public long getHistogramHourAccumulatorSize() {
    return histogramHourAccumulatorSize;
  }

  public boolean isHistogramHourAccumulatorPersisted() {
    return histogramHourAccumulatorPersisted;
  }

  public boolean isHistogramHourMemoryCache() {
    return histogramHourMemoryCache;
  }

  public String getHistogramDayListenerPorts() {
    return histogramDayListenerPorts;
  }

  public int getHistogramDayFlushSecs() {
    return histogramDayFlushSecs;
  }

  public short getHistogramDayCompression() {
    return histogramDayCompression;
  }

  public int getHistogramDayAvgKeyBytes() {
    return histogramDayAvgKeyBytes;
  }

  public int getHistogramDayAvgDigestBytes() {
    return histogramDayAvgDigestBytes;
  }

  public long getHistogramDayAccumulatorSize() {
    return histogramDayAccumulatorSize;
  }

  public boolean isHistogramDayAccumulatorPersisted() {
    return histogramDayAccumulatorPersisted;
  }

  public boolean isHistogramDayMemoryCache() {
    return histogramDayMemoryCache;
  }

  public String getHistogramDistListenerPorts() {
    return histogramDistListenerPorts;
  }

  public int getHistogramDistFlushSecs() {
    return histogramDistFlushSecs;
  }

  public short getHistogramDistCompression() {
    return histogramDistCompression;
  }

  public int getHistogramDistAvgKeyBytes() {
    return histogramDistAvgKeyBytes;
  }

  public int getHistogramDistAvgDigestBytes() {
    return histogramDistAvgDigestBytes;
  }

  public long getHistogramDistAccumulatorSize() {
    return histogramDistAccumulatorSize;
  }

  public boolean isHistogramDistAccumulatorPersisted() {
    return histogramDistAccumulatorPersisted;
  }

  public boolean isHistogramDistMemoryCache() {
    return histogramDistMemoryCache;
  }

  public String getGraphitePorts() {
    return graphitePorts;
  }

  public String getGraphiteFormat() {
    return graphiteFormat;
  }

  public String getGraphiteDelimiters() {
    return graphiteDelimiters;
  }

  public String getGraphiteFieldsToRemove() {
    return graphiteFieldsToRemove;
  }

  public String getJsonListenerPorts() {
    return jsonListenerPorts;
  }

  public String getDataDogJsonPorts() {
    return dataDogJsonPorts;
  }

  public String getDataDogRequestRelayTarget() {
    return dataDogRequestRelayTarget;
  }

  public int getDataDogRequestRelayAsyncThreads() {
    return dataDogRequestRelayAsyncThreads;
  }

  public boolean isDataDogRequestRelaySyncMode() {
    return dataDogRequestRelaySyncMode;
  }

  public boolean isDataDogProcessSystemMetrics() {
    return dataDogProcessSystemMetrics;
  }

  public boolean isDataDogProcessServiceChecks() {
    return dataDogProcessServiceChecks;
  }

  public String getWriteHttpJsonListenerPorts() {
    return writeHttpJsonListenerPorts;
  }

  public String getOtlpGrpcListenerPorts() {
    return otlpGrpcListenerPorts;
  }

  public String getOtlpHttpListenerPorts() {
    return otlpHttpListenerPorts;
  }

  public boolean isOtlpResourceAttrsOnMetricsIncluded() {
    return otlpResourceAttrsOnMetricsIncluded;
  }

  public boolean isOtlpAppTagsOnMetricsIncluded() {
    return otlpAppTagsOnMetricsIncluded;
  }

  public int getFilebeatPort() {
    return filebeatPort;
  }

  public int getRawLogsPort() {
    return rawLogsPort;
  }

  public int getRawLogsMaxReceivedLength() {
    return rawLogsMaxReceivedLength;
  }

  public int getRawLogsHttpBufferSize() {
    return rawLogsHttpBufferSize;
  }

  public String getLogsIngestionConfigFile() {
    return logsIngestionConfigFile;
  }

  public String getHostname() {
    return hostname;
  }

  public String getProxyname() {
    return proxyname;
  }

  public String getIdFile() {
    return idFile;
  }

  public String getAllowRegex() {
    return allowRegex;
  }

  public String getBlockRegex() {
    return blockRegex;
  }

  public String getOpentsdbPorts() {
    return opentsdbPorts;
  }

  public String getOpentsdbAllowRegex() {
    return opentsdbAllowRegex;
  }

  public String getOpentsdbBlockRegex() {
    return opentsdbBlockRegex;
  }

  public String getPicklePorts() {
    return picklePorts;
  }

  public String getTraceListenerPorts() {
    return traceListenerPorts;
  }

  public String getTraceJaegerListenerPorts() {
    return traceJaegerListenerPorts;
  }

  public String getTraceJaegerHttpListenerPorts() {
    return traceJaegerHttpListenerPorts;
  }

  public String getTraceJaegerGrpcListenerPorts() {
    return traceJaegerGrpcListenerPorts;
  }

  public String getTraceJaegerApplicationName() {
    return traceJaegerApplicationName;
  }

  public String getTraceZipkinListenerPorts() {
    return traceZipkinListenerPorts;
  }

  public String getTraceZipkinApplicationName() {
    return traceZipkinApplicationName;
  }

  public String getCustomTracingListenerPorts() {
    return customTracingListenerPorts;
  }

  public String getCustomTracingApplicationName() {
    return customTracingApplicationName;
  }

  public String getCustomTracingServiceName() {
    return customTracingServiceName;
  }

  public double getTraceSamplingRate() {
    return traceSamplingRate;
  }

  public int getTraceSamplingDuration() {
    return traceSamplingDuration;
  }

  public Set<String> getTraceDerivedCustomTagKeys() {
    Set<String> customTagKeys =
        new HashSet<>(
            Splitter.on(",")
                .trimResults()
                .omitEmptyStrings()
                .splitToList(ObjectUtils.firstNonNull(traceDerivedCustomTagKeys, "")));
    customTagKeys.add(SPAN_KIND.getKey()); // add span.kind tag by default
    return customTagKeys;
  }

  public boolean isBackendSpanHeadSamplingPercentIgnored() {
    return backendSpanHeadSamplingPercentIgnored;
  }

  public String getPushRelayListenerPorts() {
    return pushRelayListenerPorts;
  }

  public boolean isPushRelayHistogramAggregator() {
    return pushRelayHistogramAggregator;
  }

  public long getPushRelayHistogramAggregatorAccumulatorSize() {
    return pushRelayHistogramAggregatorAccumulatorSize;
  }

  public int getPushRelayHistogramAggregatorFlushSecs() {
    return pushRelayHistogramAggregatorFlushSecs;
  }

  public short getPushRelayHistogramAggregatorCompression() {
    return pushRelayHistogramAggregatorCompression;
  }

  public boolean isSplitPushWhenRateLimited() {
    return splitPushWhenRateLimited;
  }

  public double getRetryBackoffBaseSeconds() {
    return retryBackoffBaseSeconds;
  }

  public List<String> getCustomSourceTags() {
    // create List of custom tags from the configuration string
    Set<String> tagSet = new LinkedHashSet<>();
    Splitter.on(",")
        .trimResults()
        .omitEmptyStrings()
        .split(customSourceTags)
        .forEach(
            x -> {
              if (!tagSet.add(x)) {
                logger.warning(
                    "Duplicate tag " + x + " specified in customSourceTags config setting");
              }
            });
    return new ArrayList<>(tagSet);
  }

  public List<String> getCustomTimestampTags() {
    // create List of timestamp tags from the configuration string
    Set<String> tagSet = new LinkedHashSet<>();
    Splitter.on(",")
        .trimResults()
        .omitEmptyStrings()
        .split(customTimestampTags)
        .forEach(
            x -> {
              if (!tagSet.add(x)) {
                logger.warning(
                    "Duplicate tag " + x + " specified in customTimestampTags config setting");
              }
            });
    return new ArrayList<>(tagSet);
  }

  public List<String> getCustomMessageTags() {
    // create List of message tags from the configuration string
    Set<String> tagSet = new LinkedHashSet<>();
    Splitter.on(",")
        .trimResults()
        .omitEmptyStrings()
        .split(customMessageTags)
        .forEach(
            x -> {
              if (!tagSet.add(x)) {
                logger.warning(
                    "Duplicate tag " + x + " specified in customMessageTags config setting");
              }
            });
    return new ArrayList<>(tagSet);
  }

  public List<String> getCustomApplicationTags() {
    // create List of application tags from the configuration string
    Set<String> tagSet = new LinkedHashSet<>();
    Splitter.on(",")
        .trimResults()
        .omitEmptyStrings()
        .split(customApplicationTags)
        .forEach(
            x -> {
              if (!tagSet.add(x)) {
                logger.warning(
                    "Duplicate tag " + x + " specified in customApplicationTags config setting");
              }
            });
    return new ArrayList<>(tagSet);
  }

  public List<String> getCustomServiceTags() {
    // create List of service tags from the configuration string
    Set<String> tagSet = new LinkedHashSet<>();
    Splitter.on(",")
        .trimResults()
        .omitEmptyStrings()
        .split(customServiceTags)
        .forEach(
            x -> {
              if (!tagSet.add(x)) {
                logger.warning(
                    "Duplicate tag " + x + " specified in customServiceTags config setting");
              }
            });
    return new ArrayList<>(tagSet);
  }

  public List<String> getCustomExceptionTags() {
    Set<String> tagSet = new LinkedHashSet<>();
    Splitter.on(",")
        .trimResults()
        .omitEmptyStrings()
        .split(customExceptionTags)
        .forEach(
            x -> {
              if (!tagSet.add(x)) {
                logger.warning(
                    "Duplicate tag " + x + " specified in customExceptionTags config setting");
              }
            });
    return new ArrayList<>(tagSet);
  }

  public List<String> getCustomLevelTags() {
    // create List of level tags from the configuration string
    Set<String> tagSet = new LinkedHashSet<>();
    Splitter.on(",")
        .trimResults()
        .omitEmptyStrings()
        .split(customLevelTags)
        .forEach(
            x -> {
              if (!tagSet.add(x)) {
                logger.warning(
                    "Duplicate tag " + x + " specified in customLevelTags config setting");
              }
            });
    return new ArrayList<>(tagSet);
  }

  public Map<String, String> getAgentMetricsPointTags() {
    //noinspection UnstableApiUsage
    return agentMetricsPointTags == null
        ? Collections.emptyMap()
        : Splitter.on(",")
            .trimResults()
            .omitEmptyStrings()
            .withKeyValueSeparator("=")
            .split(agentMetricsPointTags);
  }

  public boolean isEphemeral() {
    return ephemeral;
  }

  public boolean isDisableRdnsLookup() {
    return disableRdnsLookup;
  }

  public boolean isGzipCompression() {
    return gzipCompression;
  }

  public int getGzipCompressionLevel() {
    return gzipCompressionLevel;
  }

  public int getSoLingerTime() {
    return soLingerTime;
  }

  public String getProxyHost() {
    return proxyHost;
  }

  public int getProxyPort() {
    return proxyPort;
  }

  public String getProxyUser() {
    return proxyUser;
  }

  public String getProxyPassword() {
    return proxyPassword;
  }

  public String getHttpUserAgent() {
    return httpUserAgent;
  }

  public int getHttpConnectTimeout() {
    return httpConnectTimeout;
  }

  public int getHttpRequestTimeout() {
    return httpRequestTimeout;
  }

  public int getHttpMaxConnTotal() {
    return httpMaxConnTotal;
  }

  public int getHttpMaxConnPerRoute() {
    return httpMaxConnPerRoute;
  }

  public int getHttpAutoRetries() {
    return httpAutoRetries;
  }

  public String getPreprocessorConfigFile() {
    return preprocessorConfigFile;
  }

  public int getDataBackfillCutoffHours() {
    return dataBackfillCutoffHours;
  }

  public int getDataPrefillCutoffHours() {
    return dataPrefillCutoffHours;
  }

  public TokenValidationMethod getAuthMethod() {
    return authMethod;
  }

  public String getAuthTokenIntrospectionServiceUrl() {
    return authTokenIntrospectionServiceUrl;
  }

  public String getAuthTokenIntrospectionAuthorizationHeader() {
    return authTokenIntrospectionAuthorizationHeader;
  }

  public int getAuthResponseRefreshInterval() {
    return authResponseRefreshInterval;
  }

  public int getAuthResponseMaxTtl() {
    return authResponseMaxTtl;
  }

  public String getAuthStaticToken() {
    return authStaticToken;
  }

  public int getAdminApiListenerPort() {
    return adminApiListenerPort;
  }

  public String getAdminApiRemoteIpAllowRegex() {
    return adminApiRemoteIpAllowRegex;
  }

  public String getHttpHealthCheckPorts() {
    return httpHealthCheckPorts;
  }

  public boolean isHttpHealthCheckAllPorts() {
    return httpHealthCheckAllPorts;
  }

  public String getHttpHealthCheckPath() {
    return httpHealthCheckPath;
  }

  public String getHttpHealthCheckResponseContentType() {
    return httpHealthCheckResponseContentType;
  }

  public int getHttpHealthCheckPassStatusCode() {
    return httpHealthCheckPassStatusCode;
  }

  public String getHttpHealthCheckPassResponseBody() {
    return httpHealthCheckPassResponseBody;
  }

  public int getHttpHealthCheckFailStatusCode() {
    return httpHealthCheckFailStatusCode;
  }

  public String getHttpHealthCheckFailResponseBody() {
    return httpHealthCheckFailResponseBody;
  }

  public long getDeltaCountersAggregationIntervalSeconds() {
    return deltaCountersAggregationIntervalSeconds;
  }

  public String getDeltaCountersAggregationListenerPorts() {
    return deltaCountersAggregationListenerPorts;
  }

  @JsonIgnore
  public TimeProvider getTimeProvider() {
    return timeProvider;
  }

  public String getPrivateCertPath() {
    return privateCertPath;
  }

  public String getPrivateKeyPath() {
    return privateKeyPath;
  }

  public String getTlsPorts() {
    return tlsPorts;
  }

  public boolean isTrafficShaping() {
    return trafficShaping;
  }

  public int getTrafficShapingWindowSeconds() {
    return trafficShapingWindowSeconds;
  }

  public double getTrafficShapingHeadroom() {
    return trafficShapingHeadroom;
  }

  public Map<String, TenantInfo> getMulticastingTenantList() {
    return multicastingTenantList;
  }

  public List<String> getCorsEnabledPorts() {
    return Splitter.on(",").trimResults().omitEmptyStrings().splitToList(corsEnabledPorts);
  }

  public List<String> getCorsOrigin() {
    return Splitter.on(",").trimResults().omitEmptyStrings().splitToList(corsOrigin);
  }

  public boolean isCorsAllowNullOrigin() {
    return corsAllowNullOrigin;
  }

  public String getLogServerIngestionToken() {
    return logServerIngestionToken;
  }

  public String getLogServerIngestionURL() {
    return logServerIngestionURL;
  }

  public boolean enableHyperlogsConvergedCsp() {
    return enableHyperlogsConvergedCsp;
  }

  public void setEnableHyperlogsConvergedCsp(boolean enableHyperlogsConvergedCsp) {
    this.enableHyperlogsConvergedCsp = enableHyperlogsConvergedCsp;
  }

  public boolean receivedLogServerDetails() {
    return receivedLogServerDetails;
  }

  public void setReceivedLogServerDetails(boolean receivedLogServerDetails) {
    this.receivedLogServerDetails = receivedLogServerDetails;
  }

  @Override
  public void verifyAndInit() {
    throw new UnsupportedOperationException("not implemented");
  }

  // TODO: review this options that are only available on the config file.
  private void configFileExtraArguments(ReportableConfig config) {
    // Multicasting configurations
    int multicastingTenants = Integer.parseInt(config.getProperty("multicastingTenants", "0"));
    for (int i = 1; i <= multicastingTenants; i++) {
      String tenantName = config.getProperty(String.format("multicastingTenantName_%d", i), "");
      if (tenantName.equals(APIContainer.CENTRAL_TENANT_NAME)) {
        throw new IllegalArgumentException(
            "Error in multicasting endpoints initiation: "
                + "\"central\" is the reserved tenant name.");
      }
      String tenantServer = config.getProperty(String.format("multicastingServer_%d", i), "");
      String tenantToken = config.getProperty(String.format("multicastingToken_%d", i), "");
      String tenantCSPAppId = config.getProperty(String.format("multicastingCSPAppId_%d", i), "");
      String tenantCSPAppSecret =
          config.getProperty(String.format("multicastingCSPAppSecret_%d", i), "");
      String tenantCSPOrgId = config.getProperty(String.format("multicastingCSPOrgId_%d", i), "");
      String tenantCSPAPIToken =
          config.getProperty(String.format("multicastingCSPAPIToken_%d", i), "");

      // Based on the setup parameters, the pertinent tenant information object will be produced
      // using the proper proxy
      // authentication technique.
      TenantInfo tenantInfo =
          constructTenantInfoObject(
              tenantCSPAppId,
              tenantCSPAppSecret,
              tenantCSPOrgId,
              tenantCSPAPIToken,
              tenantToken,
              tenantServer);

      multicastingTenantList.put(tenantName, tenantInfo);
    }

    if (config.isDefined("avgHistogramKeyBytes")) {
      histogramMinuteAvgKeyBytes =
          histogramHourAvgKeyBytes =
              histogramDayAvgKeyBytes =
                  histogramDistAvgKeyBytes = config.getInteger("avgHistogramKeyBytes", 150);
    }

    if (config.isDefined("avgHistogramDigestBytes")) {
      histogramMinuteAvgDigestBytes =
          histogramHourAvgDigestBytes =
              histogramDayAvgDigestBytes =
                  histogramDistAvgDigestBytes = config.getInteger("avgHistogramDigestBytes", 500);
    }
    if (config.isDefined("histogramAccumulatorSize")) {
      histogramMinuteAccumulatorSize =
          histogramHourAccumulatorSize =
              histogramDayAccumulatorSize =
                  histogramDistAccumulatorSize = config.getLong("histogramAccumulatorSize", 100000);
    }
    if (config.isDefined("histogramCompression")) {
      histogramMinuteCompression =
          histogramHourCompression =
              histogramDayCompression =
                  histogramDistCompression =
                      config.getNumber("histogramCompression", null, 20, 1000).shortValue();
    }
    if (config.isDefined("persistAccumulator")) {
      histogramMinuteAccumulatorPersisted =
          histogramHourAccumulatorPersisted =
              histogramDayAccumulatorPersisted =
                  histogramDistAccumulatorPersisted =
                      config.getBoolean("persistAccumulator", false);
    }

    histogramMinuteCompression =
        config
            .getNumber("histogramMinuteCompression", histogramMinuteCompression, 20, 1000)
            .shortValue();
    histogramMinuteAvgDigestBytes = 32 + histogramMinuteCompression * 7;

    histogramHourCompression =
        config
            .getNumber("histogramHourCompression", histogramHourCompression, 20, 1000)
            .shortValue();
    histogramHourAvgDigestBytes = 32 + histogramHourCompression * 7;

    histogramDayCompression =
        config.getNumber("histogramDayCompression", histogramDayCompression, 20, 1000).shortValue();
    histogramDayAvgDigestBytes = 32 + histogramDayCompression * 7;

    histogramDistCompression =
        config
            .getNumber("histogramDistCompression", histogramDistCompression, 20, 1000)
            .shortValue();
    histogramDistAvgDigestBytes = 32 + histogramDistCompression * 7;

    proxyPassword = config.getString("proxyPassword", proxyPassword, s -> "<removed>");
    httpMaxConnTotal = Math.min(200, config.getInteger("httpMaxConnTotal", httpMaxConnTotal));
    httpMaxConnPerRoute =
        Math.min(100, config.getInteger("httpMaxConnPerRoute", httpMaxConnPerRoute));
    gzipCompressionLevel =
        config.getNumber("gzipCompressionLevel", gzipCompressionLevel, 1, 9).intValue();

    // clamp values for pushFlushMaxPoints/etc between min split size
    // (or 1 in case of source tags and events) and default batch size.
    // also make sure it is never higher than the configured rate limit.
    pushFlushMaxPoints =
        Math.max(
            Math.min(
                Math.min(
                    config.getInteger("pushFlushMaxPoints", pushFlushMaxPoints),
                    DEFAULT_BATCH_SIZE),
                (int) pushRateLimit),
            DEFAULT_MIN_SPLIT_BATCH_SIZE);
    pushFlushMaxHistograms =
        Math.max(
            Math.min(
                Math.min(
                    config.getInteger("pushFlushMaxHistograms", pushFlushMaxHistograms),
                    DEFAULT_BATCH_SIZE_HISTOGRAMS),
                (int) pushRateLimitHistograms),
            DEFAULT_MIN_SPLIT_BATCH_SIZE);
    pushFlushMaxSourceTags =
        Math.max(
            Math.min(
                Math.min(
                    config.getInteger("pushFlushMaxSourceTags", pushFlushMaxSourceTags),
                    DEFAULT_BATCH_SIZE_SOURCE_TAGS),
                (int) pushRateLimitSourceTags),
            1);
    pushFlushMaxSpans =
        Math.max(
            Math.min(
                Math.min(
                    config.getInteger("pushFlushMaxSpans", pushFlushMaxSpans),
                    DEFAULT_BATCH_SIZE_SPANS),
                (int) pushRateLimitSpans),
            DEFAULT_MIN_SPLIT_BATCH_SIZE);
    pushFlushMaxSpanLogs =
        Math.max(
            Math.min(
                Math.min(
                    config.getInteger("pushFlushMaxSpanLogs", pushFlushMaxSpanLogs),
                    DEFAULT_BATCH_SIZE_SPAN_LOGS),
                (int) pushRateLimitSpanLogs),
            DEFAULT_MIN_SPLIT_BATCH_SIZE);
    pushFlushMaxEvents =
        Math.min(
            Math.min(
                Math.max(config.getInteger("pushFlushMaxEvents", pushFlushMaxEvents), 1),
                DEFAULT_BATCH_SIZE_EVENTS),
            (int) (pushRateLimitEvents + 1));

    pushFlushMaxLogs =
        Math.max(
            Math.min(
                Math.min(
                    config.getInteger("pushFlushMaxLogs", pushFlushMaxLogs),
                    MAX_BATCH_SIZE_LOGS_PAYLOAD),
                (int) pushRateLimitLogs),
            DEFAULT_MIN_SPLIT_BATCH_SIZE_LOGS_PAYLOAD);
    pushMemoryBufferLimitLogs =
        Math.max(
            config.getInteger("pushMemoryBufferLimitLogs", pushMemoryBufferLimitLogs),
            pushFlushMaxLogs);

    pushMemoryBufferLimit =
        Math.max(
            config.getInteger("pushMemoryBufferLimit", pushMemoryBufferLimit), pushFlushMaxPoints);
    retryBackoffBaseSeconds =
        Math.max(
            Math.min(
                config.getDouble("retryBackoffBaseSeconds", retryBackoffBaseSeconds),
                MAX_RETRY_BACKOFF_BASE_SECONDS),
            1.0);
  }

  /**
   * Parse commandline arguments into {@link ProxyConfig} object.
   *
   * @param args arguments to parse
   * @param programName program name (to display help)
   * @return true if proxy should continue, false if proxy should terminate.
   * @throws ParameterException if configuration parsing failed
   */
  public boolean parseArguments(String[] args, String programName) throws ParameterException {
    JCommander jc =
        JCommander.newBuilder()
            .programName(programName)
            .addObject(this)
            .allowParameterOverwriting(true)
            .acceptUnknownOptions(true)
            .build();

    // Command line arguments
    jc.parse(args);

    if (this.isVersion()) {
      return false;
    }
    if (this.isHelp()) {
      jc.usage();
      return false;
    }

    detectModifiedOptions(Arrays.stream(args).filter(s -> s.startsWith("-")), modifyByArgs);
    String argsStr =
        modifyByArgs.stream().map(field -> field.getName()).collect(Collectors.joining(", "));
    logger.info("modifyByArgs: " + argsStr);

    // Config file
    if (pushConfigFile != null) {
      ReportableConfig confFile = new ReportableConfig();
      List<String> fileArgs = new ArrayList<>();
      try {
        confFile.load(Files.newInputStream(Paths.get(pushConfigFile)));
      } catch (Throwable exception) {
        logger.severe("Could not load configuration file " + pushConfigFile);
        throw new RuntimeException(exception.getMessage());
      }

      confFile.entrySet().stream()
          .filter(entry -> !entry.getKey().toString().startsWith("multicasting"))
          .forEach(
              entry -> {
                fileArgs.add("--" + entry.getKey().toString());
                fileArgs.add(entry.getValue().toString());
              });

      jc.parse(fileArgs.toArray(new String[0]));
      detectModifiedOptions(fileArgs.stream().filter(s -> s.startsWith("-")), modifyByFile);
      String fileStr =
          modifyByFile.stream().map(field -> field.getName()).collect(Collectors.joining(", "));
      logger.info("modifyByFile: " + fileStr);
      modifyByArgs.removeAll(modifyByFile); // argument are override by the config file
      configFileExtraArguments(confFile);
    }

    TenantInfo tenantInfo =
        constructTenantInfoObject(cspAppId, cspAppSecret, cspOrgId, cspAPIToken, token, server);
    multicastingTenantList.put(APIContainer.CENTRAL_TENANT_NAME, tenantInfo);

    logger.info("Unparsed arguments: " + Joiner.on(", ").join(jc.getUnknownOptions()));

    String FQDN = getLocalHostName();
    if (!hostname.equals(FQDN)) {
      logger.warning(
          "Deprecated field hostname specified in config setting. Please use "
              + "proxyname config field to set proxy name.");
      if (proxyname.equals(FQDN)) proxyname = hostname;
    }
    logger.info("Using proxyname:'" + proxyname + "' hostname:'" + hostname + "'");

    if (httpUserAgent == null) {
      httpUserAgent = "Wavefront-Proxy/" + getBuildVersion();
    }

    // TODO: deprecate this
    createConfigMetrics();

    List<String> cfgStrs = new ArrayList<>();
    List<Field> cfg = new ArrayList<>();
    cfg.addAll(modifyByArgs);
    cfg.addAll(modifyByFile);
    cfg.stream()
        .forEach(
            field -> {
              Optional<ProxyConfigOption> option =
                  Arrays.stream(field.getAnnotationsByType(ProxyConfigOption.class)).findFirst();
              boolean hide = option.isPresent() && option.get().hide();
              if (!hide) {
                boolean secret = option.isPresent() && option.get().secret();
                try {
                  boolean arg = !modifyByFile.contains(field);
                  cfgStrs.add(
                      "\t"
                          + (arg ? "* " : "  ")
                          + field.getName()
                          + " = "
                          + (secret ? "******" : field.get(this)));
                } catch (IllegalAccessException e) {
                  throw new RuntimeException(e);
                }
              }
            });
    logger.info("Config: (* command line argument)");
    for (String cfgStr : cfgStrs) {
      logger.info(cfgStr);
    }

    return true;
  }

  private void createConfigMetrics() {
    Field[] fields = this.getClass().getDeclaredFields();
    for (Field field : fields) {
      Optional<Parameter> parameter =
          Arrays.stream(field.getAnnotationsByType(Parameter.class)).findFirst();
      Optional<ProxyConfigOption> option =
          Arrays.stream(field.getAnnotationsByType(ProxyConfigOption.class)).findFirst();
      boolean hide = option.isPresent() && option.get().hide();
      if (parameter.isPresent() && !hide) {
        MetricName name = new MetricName("config", "", field.getName());
        try {
          Class<?> type = (Class<?>) field.getGenericType();
          if (type.isAssignableFrom(String.class)) {
            String val = (String) field.get(this);
            if (StringUtils.isNotBlank(val)) {
              name = new TaggedMetricName(name.getGroup(), name.getName(), "value", val);
              reportGauge(1, name);
            } else {
              reportGauge(0, name);
            }
          } else if (type.isEnum()) {
            String val = field.get(this).toString();
            name = new TaggedMetricName(name.getGroup(), name.getName(), "value", val);
            reportGauge(1, name);
          } else if (type.isAssignableFrom(boolean.class)) {
            Boolean val = (Boolean) field.get(this);
            reportGauge(val.booleanValue() ? 1 : 0, name);
          } else if (type.isAssignableFrom(int.class)) {
            reportGauge((int) field.get(this), name);
          } else if (type.isAssignableFrom(double.class)) {
            reportGauge((double) field.get(this), name);
          } else if (type.isAssignableFrom(long.class)) {
            reportGauge((long) field.get(this), name);
          } else if (type.isAssignableFrom(short.class)) {
            reportGauge((short) field.get(this), name);
          } else {
            throw new RuntimeException("--- " + field.getType());
          }
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private void detectModifiedOptions(Stream<String> args, List<Field> list) {
    args.forEach(
        arg -> {
          Field[] fields = this.getClass().getSuperclass().getDeclaredFields();
          list.addAll(
              Arrays.stream(fields)
                  .filter(
                      field -> {
                        Optional<Parameter> parameter =
                            Arrays.stream(field.getAnnotationsByType(Parameter.class)).findFirst();
                        if (parameter.isPresent()) {
                          String[] names = parameter.get().names();
                          if (Arrays.asList(names).contains(arg)) {
                            return true;
                          }
                        }
                        return false;
                      })
                  .collect(Collectors.toList()));
        });
  }

  @JsonIgnore
  public JsonNode getJsonConfig() {
    Map<Categories, Map<SubCategories, Set<ProxyConfigOptionDescriptor>>> cfg =
        new TreeMap<>(Comparator.comparingInt(Categories::getOrder));
    for (Field field : this.getClass().getSuperclass().getDeclaredFields()) {
      Optional<ProxyConfigOption> option =
          Arrays.stream(field.getAnnotationsByType(ProxyConfigOption.class)).findFirst();
      Optional<Parameter> parameter =
          Arrays.stream(field.getAnnotationsByType(Parameter.class)).findFirst();
      if (parameter.isPresent()) {
        ProxyConfigOptionDescriptor data = new ProxyConfigOptionDescriptor();
        data.name =
            Arrays.stream(parameter.get().names())
                .max(Comparator.comparingInt(String::length))
                .orElseGet(() -> field.getName())
                .replaceAll("--", "");
        data.description = parameter.get().description();
        data.order = parameter.get().order() == -1 ? 99999 : parameter.get().order();
        try {
          Object val = field.get(this);
          data.value = val != null ? val.toString() : "null";
        } catch (IllegalAccessException e) {
          logger.severe(e.toString());
        }

        if (modifyByArgs.contains(field)) {
          data.modifyBy = "Argument";
        } else if (modifyByFile.contains(field)) {
          data.modifyBy = "Config file";
        }

        if (option.isPresent()) {
          Categories category = option.get().category();
          SubCategories subCategory = option.get().subCategory();
          if (!option.get().hide()) {
            Set<ProxyConfigOptionDescriptor> options =
                cfg.computeIfAbsent(
                        category,
                        s -> new TreeMap<>(Comparator.comparingInt(SubCategories::getOrder)))
                    .computeIfAbsent(subCategory, s -> new TreeSet<>());
            options.add(data);
          }
        } else {
          throw new RuntimeException(
              "All options need 'ProxyConfigOption' annotation (" + data.name + ") !!");
        }
      }
    }
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.convertValue(cfg, JsonNode.class);
    return node;
  }

  public static class TokenValidationMethodConverter
      implements IStringConverter<TokenValidationMethod> {
    @Override
    public TokenValidationMethod convert(String value) {
      TokenValidationMethod convertedValue = TokenValidationMethod.fromString(value);
      if (convertedValue == null) {
        throw new ParameterException("Unknown token validation method value: " + value);
      }
      return convertedValue;
    }
  }

  public static class TaskQueueLevelConverter implements IStringConverter<TaskQueueLevel> {
    @Override
    public TaskQueueLevel convert(String value) {
      TaskQueueLevel convertedValue = TaskQueueLevel.fromString(value);
      if (convertedValue == null) {
        throw new ParameterException("Unknown task queue level: " + value);
      }
      return convertedValue;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public static class ProxyConfigOptionDescriptor implements Comparable {
    public String name, description, value, modifyBy;
    public int order = 0;

    @Override
    public int compareTo(@NotNull Object o) {
      ProxyConfigOptionDescriptor other = (ProxyConfigOptionDescriptor) o;
      if (this.order == other.order) {
        return this.name.compareTo(other.name);
      }
      return Integer.compare(this.order, other.order);
    }
  }

  /**
   * Helper function to construct tenant info {@link TenantInfo} object based on input parameters.
   *
   * @param appId the CSP OAuth server to server app id.
   * @param appSecret the CSP OAuth server to server app secret.
   * @param cspOrgId the CSP organisation id.
   * @param cspAPIToken the CSP API token.
   * @param token the Wavefront API token.
   * @param server the server url.
   * @return constructed tenant info {@link TenantInfo} object.
   */
  private TenantInfo constructTenantInfoObject(
      @Nullable final String appId,
      @Nullable final String appSecret,
      @Nullable final String cspOrgId,
      @Nullable final String cspAPIToken,
      @Nonnull final String token,
      @Nonnull final String server) {

    TenantInfo tenantInfo;
    if (StringUtils.isNotBlank(appId)
        && StringUtils.isNotBlank(appSecret)
        && StringUtils.isNotBlank(cspOrgId)) {
      tenantInfo = new TenantInfo(appId, appSecret, cspOrgId, server, CSP_CLIENT_CREDENTIALS);
      logger.info(
          "The proxy selected the CSP OAuth server to server app credentials for further authentication. For the server "
              + server);
    } else if (StringUtils.isNotBlank(cspAPIToken)) {
      tenantInfo = new TenantInfo(cspAPIToken, server, CSP_API_TOKEN);
      logger.info(
          "The proxy selected the CSP api token for further authentication. For the server "
              + server);
    } else {
      tenantInfo = new TenantInfo(token, server, WAVEFRONT_API_TOKEN);
      logger.info(
          "The proxy selected the Wavefront api token for further authentication. For the server "
              + server);
    }
    return tenantInfo;
  }
}

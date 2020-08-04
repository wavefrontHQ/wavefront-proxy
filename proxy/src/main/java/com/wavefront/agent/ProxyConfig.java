package com.wavefront.agent;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.wavefront.agent.auth.TokenValidationMethod;
import com.wavefront.agent.config.Configuration;
import com.wavefront.agent.config.ReportableConfig;
import com.wavefront.agent.data.TaskQueueLevel;
import com.wavefront.common.TimeProvider;
import org.apache.commons.lang3.ObjectUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import static com.wavefront.agent.data.EntityProperties.DEFAULT_BATCH_SIZE;
import static com.wavefront.agent.data.EntityProperties.DEFAULT_BATCH_SIZE_EVENTS;
import static com.wavefront.agent.data.EntityProperties.DEFAULT_BATCH_SIZE_HISTOGRAMS;
import static com.wavefront.agent.data.EntityProperties.DEFAULT_BATCH_SIZE_SOURCE_TAGS;
import static com.wavefront.agent.data.EntityProperties.DEFAULT_BATCH_SIZE_SPANS;
import static com.wavefront.agent.data.EntityProperties.DEFAULT_BATCH_SIZE_SPAN_LOGS;
import static com.wavefront.agent.data.EntityProperties.DEFAULT_FLUSH_INTERVAL;
import static com.wavefront.agent.data.EntityProperties.DEFAULT_FLUSH_THREADS_EVENTS;
import static com.wavefront.agent.data.EntityProperties.DEFAULT_FLUSH_THREADS_SOURCE_TAGS;
import static com.wavefront.agent.data.EntityProperties.DEFAULT_MIN_SPLIT_BATCH_SIZE;
import static com.wavefront.agent.data.EntityProperties.DEFAULT_RETRY_BACKOFF_BASE_SECONDS;
import static com.wavefront.agent.data.EntityProperties.DEFAULT_SPLIT_PUSH_WHEN_RATE_LIMITED;
import static com.wavefront.agent.data.EntityProperties.NO_RATE_LIMIT;
import static com.wavefront.common.Utils.getLocalHostName;
import static com.wavefront.common.Utils.getBuildVersion;

import static io.opentracing.tag.Tags.SPAN_KIND;

/**
 * Proxy configuration (refactored from {@link com.wavefront.agent.AbstractAgent}).
 *
 * @author vasily@wavefront.com
 */
@SuppressWarnings("CanBeFinal")
public class ProxyConfig extends Configuration {
  private static final Logger logger = Logger.getLogger(ProxyConfig.class.getCanonicalName());
  private static final double MAX_RETRY_BACKOFF_BASE_SECONDS = 60.0;
  private static final int GRAPHITE_LISTENING_PORT = 2878;

  @Parameter(names = {"--help"}, help = true)
  boolean help = false;

  @Parameter(names = {"--version"}, description = "Print version and exit.", order = 0)
  boolean version = false;

  @Parameter(names = {"-f", "--file"}, description =
      "Proxy configuration file", order = 1)
  String pushConfigFile = null;

  @Parameter(names = {"-p", "--prefix"}, description =
      "Prefix to prepend to all push metrics before reporting.")
  String prefix = null;

  @Parameter(names = {"-t", "--token"}, description =
      "Token to auto-register proxy with an account", order = 3)
  String token = null;

  @Parameter(names = {"--testLogs"}, description = "Run interactive session for crafting logsIngestionConfig.yaml")
  boolean testLogs = false;

  @Parameter(names = {"--testPreprocessorForPort"}, description = "Run interactive session for " +
      "testing preprocessor rules for specified port")
  String testPreprocessorForPort = null;

  @Parameter(names = {"--testSpanPreprocessorForPort"}, description = "Run interactive session " +
      "for testing preprocessor span rules for specifierd port")
  String testSpanPreprocessorForPort = null;

  @Parameter(names = {"-h", "--host"}, description = "Server URL", order = 2)
  String server = "http://localhost:8080/api/";

  @Parameter(names = {"--buffer"}, description = "File to use for buffering transmissions " +
      "to be retried. Defaults to /var/spool/wavefront-proxy/buffer.", order = 7)
  String bufferFile = "/var/spool/wavefront-proxy/buffer";

  @Parameter(names = {"--sqsBuffer"}, description = "Use AWS SQS Based for buffering transmissions " +
      "to be retried. Defaults to False")
  boolean sqsQueueBuffer = false;

  @Parameter(names = {"--sqsQueueNameTemplate"}, description = "The replacement pattern to use for naming the " +
      "sqs queues. e.g. wf-proxy-{{id}}-{{entity}}-{{port}} would result in a queue named wf-proxy-id-points-2878")
  String sqsQueueNameTemplate = "wf-proxy-{{id}}-{{entity}}-{{port}}";

  @Parameter(names = {"--sqsQueueIdentifier"}, description = "An identifier for identifying these proxies in SQS")
  String sqsQueueIdentifier = null;

  @Parameter(names = {"--sqsQueueRegion"}, description = "The AWS Region name the queue will live in.")
  String sqsQueueRegion = "us-west-2";

  @Parameter(names = {"--taskQueueLevel"}, converter = TaskQueueLevelConverter.class,
      description = "Sets queueing strategy. Allowed values: MEMORY, PUSHBACK, ANY_ERROR. " +
          "Default: ANY_ERROR")
  TaskQueueLevel taskQueueLevel = TaskQueueLevel.ANY_ERROR;

  @Parameter(names = {"--exportQueuePorts"}, description = "Export queued data in plaintext " +
      "format for specified ports (comma-delimited list) and exit. Set to 'all' to export " +
      "everything. Default: none")
  String exportQueuePorts = null;

  @Parameter(names = {"--exportQueueOutputFile"}, description = "Export queued data in plaintext " +
      "format for specified ports (comma-delimited list) and exit. Default: none")
  String exportQueueOutputFile = null;

  @Parameter(names = {"--exportQueueRetainData"}, description = "Whether to retain data in the " +
      "queue during export. Defaults to true.", arity = 1)
  boolean exportQueueRetainData = true;

  @Parameter(names = {"--useNoopSender"}, description = "Run proxy in debug/performance test " +
      "mode and discard all received data. Default: false", arity = 1)
  boolean useNoopSender = false;

  @Parameter(names = {"--flushThreads"}, description = "Number of threads that flush data to the server. Defaults to" +
      "the number of processors (min. 4). Setting this value too large will result in sending batches that are too " +
      "small to the server and wasting connections. This setting is per listening port.", order = 5)
  Integer flushThreads = Math.min(16, Math.max(4, Runtime.getRuntime().availableProcessors()));

  @Parameter(names = {"--flushThreadsSourceTags"}, description = "Number of threads that send " +
      "source tags data to the server. Default: 2")
  int flushThreadsSourceTags = DEFAULT_FLUSH_THREADS_SOURCE_TAGS;

  @Parameter(names = {"--flushThreadsEvents"}, description = "Number of threads that send " +
      "event data to the server. Default: 2")
  int flushThreadsEvents = DEFAULT_FLUSH_THREADS_EVENTS;

  @Parameter(names = {"--purgeBuffer"}, description = "Whether to purge the retry buffer on start-up. Defaults to " +
      "false.", arity = 1)
  boolean purgeBuffer = false;

  @Parameter(names = {"--pushFlushInterval"}, description = "Milliseconds between batches. " +
      "Defaults to 1000 ms")
  int pushFlushInterval = DEFAULT_FLUSH_INTERVAL;

  @Parameter(names = {"--pushFlushMaxPoints"}, description = "Maximum allowed points " +
      "in a single flush. Defaults: 40000")
  int pushFlushMaxPoints = DEFAULT_BATCH_SIZE;

  @Parameter(names = {"--pushFlushMaxHistograms"}, description = "Maximum allowed histograms " +
      "in a single flush. Default: 10000")
  int pushFlushMaxHistograms = DEFAULT_BATCH_SIZE_HISTOGRAMS;

  @Parameter(names = {"--pushFlushMaxSourceTags"}, description = "Maximum allowed source tags " +
      "in a single flush. Default: 50")
  int pushFlushMaxSourceTags = DEFAULT_BATCH_SIZE_SOURCE_TAGS;

  @Parameter(names = {"--pushFlushMaxSpans"}, description = "Maximum allowed spans " +
      "in a single flush. Default: 5000")
  int pushFlushMaxSpans = DEFAULT_BATCH_SIZE_SPANS;

  @Parameter(names = {"--pushFlushMaxSpanLogs"}, description = "Maximum allowed span logs " +
      "in a single flush. Default: 1000")
  int pushFlushMaxSpanLogs = DEFAULT_BATCH_SIZE_SPAN_LOGS;

  @Parameter(names = {"--pushFlushMaxEvents"}, description = "Maximum allowed events " +
      "in a single flush. Default: 50")
  int pushFlushMaxEvents = DEFAULT_BATCH_SIZE_EVENTS;

  @Parameter(names = {"--pushRateLimit"}, description = "Limit the outgoing point rate at the proxy. Default: " +
      "do not throttle.")
  double pushRateLimit = NO_RATE_LIMIT;

  @Parameter(names = {"--pushRateLimitHistograms"}, description = "Limit the outgoing histogram " +
      "rate at the proxy. Default: do not throttle.")
  double pushRateLimitHistograms = NO_RATE_LIMIT;

  @Parameter(names = {"--pushRateLimitSourceTags"}, description = "Limit the outgoing rate " +
      "for source tags at the proxy. Default: 5 op/s")
  double pushRateLimitSourceTags = 5.0d;

  @Parameter(names = {"--pushRateLimitSpans"}, description = "Limit the outgoing tracing spans " +
      "rate at the proxy. Default: do not throttle.")
  double pushRateLimitSpans = NO_RATE_LIMIT;

  @Parameter(names = {"--pushRateLimitSpanLogs"}, description = "Limit the outgoing span logs " +
      "rate at the proxy. Default: do not throttle.")
  double pushRateLimitSpanLogs = NO_RATE_LIMIT;

  @Parameter(names = {"--pushRateLimitEvents"}, description = "Limit the outgoing rate " +
      "for events at the proxy. Default: 5 events/s")
  double pushRateLimitEvents = 5.0d;

  @Parameter(names = {"--pushRateLimitMaxBurstSeconds"}, description = "Max number of burst seconds to allow " +
      "when rate limiting to smooth out uneven traffic. Set to 1 when doing data backfills. Default: 10")
  Integer pushRateLimitMaxBurstSeconds = 10;

  @Parameter(names = {"--pushMemoryBufferLimit"}, description = "Max number of points that can stay in memory buffers" +
      " before spooling to disk. Defaults to 16 * pushFlushMaxPoints, minimum size: pushFlushMaxPoints. Setting this " +
      " value lower than default reduces memory usage but will force the proxy to spool to disk more frequently if " +
      " you have points arriving at the proxy in short bursts")
  int pushMemoryBufferLimit = 16 * pushFlushMaxPoints;

  @Parameter(names = {"--pushBlockedSamples"}, description = "Max number of blocked samples to print to log. Defaults" +
      " to 5.")
  Integer pushBlockedSamples = 5;

  @Parameter(names = {"--blockedPointsLoggerName"}, description = "Logger Name for blocked " +
      "points. " + "Default: RawBlockedPoints")
  String blockedPointsLoggerName = "RawBlockedPoints";

  @Parameter(names = {"--blockedHistogramsLoggerName"}, description = "Logger Name for blocked " +
      "histograms" + "Default: RawBlockedPoints")
  String blockedHistogramsLoggerName = "RawBlockedPoints";

  @Parameter(names = {"--blockedSpansLoggerName"}, description =
      "Logger Name for blocked spans" + "Default: RawBlockedPoints")
  String blockedSpansLoggerName = "RawBlockedPoints";

  @Parameter(names = {"--pushListenerPorts"}, description = "Comma-separated list of ports to listen on. Defaults to " +
      "2878.", order = 4)
  String pushListenerPorts = "" + GRAPHITE_LISTENING_PORT;

  @Parameter(names = {"--pushListenerMaxReceivedLength"}, description = "Maximum line length for received points in" +
      " plaintext format on Wavefront/OpenTSDB/Graphite ports. Default: 32768 (32KB)")
  Integer pushListenerMaxReceivedLength = 32768;

  @Parameter(names = {"--pushListenerHttpBufferSize"}, description = "Maximum allowed request size (in bytes) for" +
      " incoming HTTP requests on Wavefront/OpenTSDB/Graphite ports (Default: 16MB)")
  Integer pushListenerHttpBufferSize = 16 * 1024 * 1024;

  @Parameter(names = {"--traceListenerMaxReceivedLength"}, description = "Maximum line length for received spans and" +
      " span logs (Default: 1MB)")
  Integer traceListenerMaxReceivedLength = 1024 * 1024;

  @Parameter(names = {"--traceListenerHttpBufferSize"}, description = "Maximum allowed request size (in bytes) for" +
      " incoming HTTP requests on tracing ports (Default: 16MB)")
  Integer traceListenerHttpBufferSize = 16 * 1024 * 1024;

  @Parameter(names = {"--listenerIdleConnectionTimeout"}, description = "Close idle inbound connections after " +
      " specified time in seconds. Default: 300")
  int listenerIdleConnectionTimeout = 300;

  @Parameter(names = {"--memGuardFlushThreshold"}, description = "If heap usage exceeds this threshold (in percent), " +
      "flush pending points to disk as an additional OoM protection measure. Set to 0 to disable. Default: 99")
  int memGuardFlushThreshold = 98;

  @Parameter(names = {"--histogramPassthroughRecompression"},
      description = "Whether we should recompress histograms received on pushListenerPorts. " +
          "Default: true", arity = 1)
  boolean histogramPassthroughRecompression = true;

  @Parameter(names = {"--histogramStateDirectory"},
      description = "Directory for persistent proxy state, must be writable.")
  String histogramStateDirectory = "/var/spool/wavefront-proxy";

  @Parameter(names = {"--histogramAccumulatorResolveInterval"},
      description = "Interval to write-back accumulation changes from memory cache to disk in " +
          "millis (only applicable when memory cache is enabled")
  Long histogramAccumulatorResolveInterval = 5000L;

  @Parameter(names = {"--histogramAccumulatorFlushInterval"},
      description = "Interval to check for histograms to send to Wavefront in millis. " +
          "(Default: 10000)")
  Long histogramAccumulatorFlushInterval = 10000L;

  @Parameter(names = {"--histogramAccumulatorFlushMaxBatchSize"},
      description = "Max number of histograms to send to Wavefront in one flush " +
          "(Default: no limit)")
  Integer histogramAccumulatorFlushMaxBatchSize = -1;

  @Parameter(names = {"--histogramMaxReceivedLength"},
      description = "Maximum line length for received histogram data (Default: 65536)")
  Integer histogramMaxReceivedLength = 64 * 1024;

  @Parameter(names = {"--histogramHttpBufferSize"},
      description = "Maximum allowed request size (in bytes) for incoming HTTP requests on " +
          "histogram ports (Default: 16MB)")
  Integer histogramHttpBufferSize = 16 * 1024 * 1024;

  @Parameter(names = {"--histogramMinuteListenerPorts"},
      description = "Comma-separated list of ports to listen on. Defaults to none.")
  String histogramMinuteListenerPorts = "";

  @Parameter(names = {"--histogramMinuteFlushSecs"},
      description = "Number of seconds to keep a minute granularity accumulator open for " +
          "new samples.")
  Integer histogramMinuteFlushSecs = 70;

  @Parameter(names = {"--histogramMinuteCompression"},
      description = "Controls allowable number of centroids per histogram. Must be in [20;1000]")
  Short histogramMinuteCompression = 32;

  @Parameter(names = {"--histogramMinuteAvgKeyBytes"},
      description = "Average number of bytes in a [UTF-8] encoded histogram key. Generally " +
          "corresponds to a metric, source and tags concatenation.")
  Integer histogramMinuteAvgKeyBytes = 150;

  @Parameter(names = {"--histogramMinuteAvgDigestBytes"},
      description = "Average number of bytes in a encoded histogram.")
  Integer histogramMinuteAvgDigestBytes = 500;

  @Parameter(names = {"--histogramMinuteAccumulatorSize"},
      description = "Expected upper bound of concurrent accumulations, ~ #timeseries * #parallel " +
          "reporting bins")
  Long histogramMinuteAccumulatorSize = 100000L;

  @Parameter(names = {"--histogramMinuteAccumulatorPersisted"}, arity = 1,
      description = "Whether the accumulator should persist to disk")
  boolean histogramMinuteAccumulatorPersisted = false;

  @Parameter(names = {"--histogramMinuteMemoryCache"}, arity = 1,
      description = "Enabling memory cache reduces I/O load with fewer time series and higher " +
          "frequency data (more than 1 point per second per time series). Default: false")
  boolean histogramMinuteMemoryCache = false;

  @Parameter(names = {"--histogramHourListenerPorts"},
      description = "Comma-separated list of ports to listen on. Defaults to none.")
  String histogramHourListenerPorts = "";

  @Parameter(names = {"--histogramHourFlushSecs"},
      description = "Number of seconds to keep an hour granularity accumulator open for " +
          "new samples.")
  Integer histogramHourFlushSecs = 4200;

  @Parameter(names = {"--histogramHourCompression"},
      description = "Controls allowable number of centroids per histogram. Must be in [20;1000]")
  Short histogramHourCompression = 32;

  @Parameter(names = {"--histogramHourAvgKeyBytes"},
      description = "Average number of bytes in a [UTF-8] encoded histogram key. Generally " +
          " corresponds to a metric, source and tags concatenation.")
  Integer histogramHourAvgKeyBytes = 150;

  @Parameter(names = {"--histogramHourAvgDigestBytes"},
      description = "Average number of bytes in a encoded histogram.")
  Integer histogramHourAvgDigestBytes = 500;

  @Parameter(names = {"--histogramHourAccumulatorSize"},
      description = "Expected upper bound of concurrent accumulations, ~ #timeseries * #parallel " +
          "reporting bins")
  Long histogramHourAccumulatorSize = 100000L;

  @Parameter(names = {"--histogramHourAccumulatorPersisted"}, arity = 1,
      description = "Whether the accumulator should persist to disk")
  boolean histogramHourAccumulatorPersisted = false;

  @Parameter(names = {"--histogramHourMemoryCache"}, arity = 1,
      description = "Enabling memory cache reduces I/O load with fewer time series and higher " +
          "frequency data (more than 1 point per second per time series). Default: false")
  boolean histogramHourMemoryCache = false;

  @Parameter(names = {"--histogramDayListenerPorts"},
      description = "Comma-separated list of ports to listen on. Defaults to none.")
  String histogramDayListenerPorts = "";

  @Parameter(names = {"--histogramDayFlushSecs"},
      description = "Number of seconds to keep a day granularity accumulator open for new samples.")
  Integer histogramDayFlushSecs = 18000;

  @Parameter(names = {"--histogramDayCompression"},
      description = "Controls allowable number of centroids per histogram. Must be in [20;1000]")
  Short histogramDayCompression = 32;

  @Parameter(names = {"--histogramDayAvgKeyBytes"},
      description = "Average number of bytes in a [UTF-8] encoded histogram key. Generally " +
          "corresponds to a metric, source and tags concatenation.")
  Integer histogramDayAvgKeyBytes = 150;

  @Parameter(names = {"--histogramDayAvgHistogramDigestBytes"},
      description = "Average number of bytes in a encoded histogram.")
  Integer histogramDayAvgDigestBytes = 500;

  @Parameter(names = {"--histogramDayAccumulatorSize"},
      description = "Expected upper bound of concurrent accumulations, ~ #timeseries * #parallel " +
          "reporting bins")
  Long histogramDayAccumulatorSize = 100000L;

  @Parameter(names = {"--histogramDayAccumulatorPersisted"}, arity = 1,
      description = "Whether the accumulator should persist to disk")
  boolean histogramDayAccumulatorPersisted = false;

  @Parameter(names = {"--histogramDayMemoryCache"}, arity = 1,
      description = "Enabling memory cache reduces I/O load with fewer time series and higher " +
          "frequency data (more than 1 point per second per time series). Default: false")
  boolean histogramDayMemoryCache = false;

  @Parameter(names = {"--histogramDistListenerPorts"},
      description = "Comma-separated list of ports to listen on. Defaults to none.")
  String histogramDistListenerPorts = "";

  @Parameter(names = {"--histogramDistFlushSecs"},
      description = "Number of seconds to keep a new distribution bin open for new samples.")
  Integer histogramDistFlushSecs = 70;

  @Parameter(names = {"--histogramDistCompression"},
      description = "Controls allowable number of centroids per histogram. Must be in [20;1000]")
  Short histogramDistCompression = 32;

  @Parameter(names = {"--histogramDistAvgKeyBytes"},
      description = "Average number of bytes in a [UTF-8] encoded histogram key. Generally " +
          "corresponds to a metric, source and tags concatenation.")
  Integer histogramDistAvgKeyBytes = 150;

  @Parameter(names = {"--histogramDistAvgDigestBytes"},
      description = "Average number of bytes in a encoded histogram.")
  Integer histogramDistAvgDigestBytes = 500;

  @Parameter(names = {"--histogramDistAccumulatorSize"},
      description = "Expected upper bound of concurrent accumulations, ~ #timeseries * #parallel " +
          "reporting bins")
  Long histogramDistAccumulatorSize = 100000L;

  @Parameter(names = {"--histogramDistAccumulatorPersisted"}, arity = 1,
      description = "Whether the accumulator should persist to disk")
  boolean histogramDistAccumulatorPersisted = false;

  @Parameter(names = {"--histogramDistMemoryCache"}, arity = 1,
      description = "Enabling memory cache reduces I/O load with fewer time series and higher " +
          "frequency data (more than 1 point per second per time series). Default: false")
  boolean histogramDistMemoryCache = false;

  @Parameter(names = {"--graphitePorts"}, description = "Comma-separated list of ports to listen on for graphite " +
      "data. Defaults to empty list.")
  String graphitePorts = "";

  @Parameter(names = {"--graphiteFormat"}, description = "Comma-separated list of metric segments to extract and " +
      "reassemble as the hostname (1-based).")
  String graphiteFormat = "";

  @Parameter(names = {"--graphiteDelimiters"}, description = "Concatenated delimiters that should be replaced in the " +
      "extracted hostname with dots. Defaults to underscores (_).")
  String graphiteDelimiters = "_";

  @Parameter(names = {"--graphiteFieldsToRemove"}, description = "Comma-separated list of metric segments to remove (1-based)")
  String graphiteFieldsToRemove;

  @Parameter(names = {"--jsonListenerPorts", "--httpJsonPorts"}, description = "Comma-separated list of ports to " +
      "listen on for json metrics data. Binds, by default, to none.")
  String jsonListenerPorts = "";

  @Parameter(names = {"--dataDogJsonPorts"}, description = "Comma-separated list of ports to listen on for JSON " +
      "metrics data in DataDog format. Binds, by default, to none.")
  String dataDogJsonPorts = "";

  @Parameter(names = {"--dataDogRequestRelayTarget"}, description = "HTTP/HTTPS target for relaying all incoming " +
      "requests on dataDogJsonPorts to. Defaults to none (do not relay incoming requests)")
  String dataDogRequestRelayTarget = null;

  @Parameter(names = {"--dataDogRequestRelayAsyncThreads"}, description = "Max number of " +
      "in-flight HTTP requests being relayed to dataDogRequestRelayTarget. Default: 32")
  int dataDogRequestRelayAsyncThreads = 32;

  @Parameter(names = {"--dataDogRequestRelaySyncMode"}, description = "Whether we should wait " +
      "until request is relayed successfully before processing metrics. Default: false")
  boolean dataDogRequestRelaySyncMode = false;

  @Parameter(names = {"--dataDogProcessSystemMetrics"}, description = "If true, handle system metrics as reported by " +
      "DataDog collection agent. Defaults to false.", arity = 1)
  boolean dataDogProcessSystemMetrics = false;

  @Parameter(names = {"--dataDogProcessServiceChecks"}, description = "If true, convert service checks to metrics. " +
      "Defaults to true.", arity = 1)
  boolean dataDogProcessServiceChecks = true;

  @Parameter(names = {"--writeHttpJsonListenerPorts", "--writeHttpJsonPorts"}, description = "Comma-separated list " +
      "of ports to listen on for json metrics from collectd write_http json format data. Binds, by default, to none.")
  String writeHttpJsonListenerPorts = "";

  // logs ingestion
  @Parameter(names = {"--filebeatPort"}, description = "Port on which to listen for filebeat data.")
  Integer filebeatPort = 0;

  @Parameter(names = {"--rawLogsPort"}, description = "Port on which to listen for raw logs data.")
  Integer rawLogsPort = 0;

  @Parameter(names = {"--rawLogsMaxReceivedLength"}, description = "Maximum line length for received raw logs (Default: 4096)")
  Integer rawLogsMaxReceivedLength = 4096;

  @Parameter(names = {"--rawLogsHttpBufferSize"}, description = "Maximum allowed request size (in bytes) for" +
      " incoming HTTP requests with raw logs (Default: 16MB)")
  Integer rawLogsHttpBufferSize = 16 * 1024 * 1024;

  @Parameter(names = {"--logsIngestionConfigFile"}, description = "Location of logs ingestions config yaml file.")
  String logsIngestionConfigFile = "/etc/wavefront/wavefront-proxy/logsingestion.yaml";

  @Parameter(names = {"--hostname"}, description = "Hostname for the proxy. Defaults to FQDN of machine.")
  String hostname = getLocalHostName();

  @Parameter(names = {"--idFile"}, description = "File to read proxy id from. Defaults to ~/.dshell/id." +
      "This property is ignored if ephemeral=true.")
  String idFile = null;

  @Parameter(names = {"--allowRegex", "--whitelistRegex"}, description = "Regex pattern (java" +
      ".util.regex) that graphite input lines must match to be accepted")
  String allowRegex;

  @Parameter(names = {"--blockRegex", "--blacklistRegex"}, description = "Regex pattern (java" +
      ".util.regex) that graphite input lines must NOT match to be accepted")
  String blockRegex;

  @Parameter(names = {"--opentsdbPorts"}, description = "Comma-separated list of ports to listen on for opentsdb data. " +
      "Binds, by default, to none.")
  String opentsdbPorts = "";

  @Parameter(names = {"--opentsdbAllowRegex", "--opentsdbWhitelistRegex"}, description = "Regex " +
      "pattern (java.util.regex) that opentsdb input lines must match to be accepted")
  String opentsdbAllowRegex;

  @Parameter(names = {"--opentsdbBlockRegex", "--opentsdbBlacklistRegex"}, description = "Regex " +
      "pattern (java.util.regex) that opentsdb input lines must NOT match to be accepted")
  String opentsdbBlockRegex;

  @Parameter(names = {"--picklePorts"}, description = "Comma-separated list of ports to listen on for pickle protocol " +
      "data. Defaults to none.")
  String picklePorts;

  @Parameter(names = {"--traceListenerPorts"}, description = "Comma-separated list of ports to listen on for trace " +
      "data. Defaults to none.")
  String traceListenerPorts;

  @Parameter(names = {"--traceJaegerListenerPorts"}, description = "Comma-separated list of ports on which to listen " +
      "on for jaeger thrift formatted data over TChannel protocol. Defaults to none.")
  String traceJaegerListenerPorts;

  @Parameter(names = {"--traceJaegerHttpListenerPorts"}, description = "Comma-separated list of ports on which to listen " +
      "on for jaeger thrift formatted data over HTTP. Defaults to none.")
  String traceJaegerHttpListenerPorts;

  @Parameter(names = {"--traceJaegerGrpcListenerPorts"}, description = "Comma-separated list of ports on which to listen " +
      "on for jaeger Protobuf formatted data over gRPC. Defaults to none.")
  String traceJaegerGrpcListenerPorts;

  @Parameter(names = {"--traceJaegerApplicationName"}, description = "Application name for Jaeger. Defaults to Jaeger.")
  String traceJaegerApplicationName;

  @Parameter(names = {"--traceZipkinListenerPorts"}, description = "Comma-separated list of ports on which to listen " +
      "on for zipkin trace data over HTTP. Defaults to none.")
  String traceZipkinListenerPorts;

  @Parameter(names = {"--traceZipkinApplicationName"}, description = "Application name for Zipkin. Defaults to Zipkin.")
  String traceZipkinApplicationName;

  @Parameter(names = {"--customTracingListenerPorts"},
      description = "Comma-separated list of ports to listen on spans from level 1 SDK. Helps " +
          "derive RED metrics and for the span and heartbeat for corresponding application at " +
          "proxy. Defaults: none")
  String customTracingListenerPorts = "";

  @Parameter(names = {"--customTracingApplicationName"}, description = "Application name to use " +
      "for spans sent to customTracingListenerPorts when span doesn't have application tag. " +
      "Defaults to defaultApp.")
  String customTracingApplicationName;

  @Parameter(names = {"--customTracingServiceName"}, description = "Service name to use for spans" +
      " sent to customTracingListenerPorts when span doesn't have service tag. " +
      "Defaults to defaultService.")
  String customTracingServiceName;

  @Parameter(names = {"--traceSamplingRate"}, description = "Value between 0.0 and 1.0. " +
      "Defaults to 1.0 (allow all spans).")
  double traceSamplingRate = 1.0d;

  @Parameter(names = {"--traceSamplingDuration"}, description = "Sample spans by duration in " +
      "milliseconds. " + "Defaults to 0 (ignore duration based sampling).")
  Integer traceSamplingDuration = 0;

  @Parameter(names = {"--traceDerivedCustomTagKeys"}, description = "Comma-separated " +
      "list of custom tag keys for trace derived RED metrics.")
  String traceDerivedCustomTagKeys;

  @Parameter(names = {"--traceAlwaysSampleErrors"}, description = "Always sample spans with error tag (set to true) " +
      "ignoring other sampling configuration. Defaults to true.", arity = 1)
  boolean traceAlwaysSampleErrors = true;

  @Parameter(names = {"--pushRelayListenerPorts"}, description = "Comma-separated list of ports on which to listen " +
      "on for proxy chaining data. For internal use. Defaults to none.")
  String pushRelayListenerPorts;

  @Parameter(names = {"--pushRelayHistogramAggregator"}, description = "If true, aggregate " +
      "histogram distributions received on the relay port. Default: false", arity = 1)
  boolean pushRelayHistogramAggregator = false;

  @Parameter(names = {"--pushRelayHistogramAggregatorAccumulatorSize"},
      description = "Expected upper bound of concurrent accumulations, ~ #timeseries * #parallel " +
          "reporting bins")
  Long pushRelayHistogramAggregatorAccumulatorSize = 100000L;

  @Parameter(names = {"--pushRelayHistogramAggregatorFlushSecs"},
      description = "Number of seconds to keep accumulator open for new samples.")
  Integer pushRelayHistogramAggregatorFlushSecs = 70;

  @Parameter(names = {"--pushRelayHistogramAggregatorCompression"},
      description = "Controls allowable number of centroids per histogram. Must be in [20;1000] " +
          "range. Default: 32")
  Short pushRelayHistogramAggregatorCompression = 32;

  @Parameter(names = {"--splitPushWhenRateLimited"}, description = "Whether to split the push " +
      "batch size when the push is rejected by Wavefront due to rate limit.  Default false.",
      arity = 1)
  boolean splitPushWhenRateLimited = DEFAULT_SPLIT_PUSH_WHEN_RATE_LIMITED;

  @Parameter(names = {"--retryBackoffBaseSeconds"}, description = "For exponential backoff " +
      "when retry threads are throttled, the base (a in a^b) in seconds.  Default 2.0")
  double retryBackoffBaseSeconds = DEFAULT_RETRY_BACKOFF_BASE_SECONDS;

  @Parameter(names = {"--customSourceTags"}, description = "Comma separated list of point tag " +
      "keys that should be treated as the source in Wavefront in the absence of a tag named " +
      "`source` or `host`. Default: fqdn")
  String customSourceTags = "fqdn";

  @Parameter(names = {"--agentMetricsPointTags"}, description = "Additional point tags and their " +
      " respective values to be included into internal agent's metrics " +
      "(comma-separated list, ex: dc=west,env=prod). Default: none")
  String agentMetricsPointTags = null;

  @Parameter(names = {"--ephemeral"}, arity = 1, description = "If true, this proxy is removed " +
      "from Wavefront after 24 hours of inactivity. Default: true")
  boolean ephemeral = true;

  @Parameter(names = {"--disableRdnsLookup"}, arity = 1, description = "When receiving" +
      " Wavefront-formatted data without source/host specified, use remote IP address as source " +
      "instead of trying to resolve the DNS name. Default false.")
  boolean disableRdnsLookup = false;

  @Parameter(names = {"--gzipCompression"}, arity = 1, description = "If true, enables gzip " +
      "compression for traffic sent to Wavefront (Default: true)")
  boolean gzipCompression = true;

  @Parameter(names = {"--gzipCompressionLevel"}, description = "If gzipCompression is enabled, " +
      "sets compression level (1-9). Higher compression levels use more CPU. Default: 4")
  int gzipCompressionLevel = 4;

  @Parameter(names = {"--soLingerTime"}, description = "If provided, enables SO_LINGER with the specified linger time in seconds (default: SO_LINGER disabled)")
  Integer soLingerTime = -1;

  @Parameter(names = {"--proxyHost"}, description = "Proxy host for routing traffic through a http proxy")
  String proxyHost = null;

  @Parameter(names = {"--proxyPort"}, description = "Proxy port for routing traffic through a http proxy")
  Integer proxyPort = 0;

  @Parameter(names = {"--proxyUser"}, description = "If proxy authentication is necessary, this is the username that will be passed along")
  String proxyUser = null;

  @Parameter(names = {"--proxyPassword"}, description = "If proxy authentication is necessary, this is the password that will be passed along")
  String proxyPassword = null;

  @Parameter(names = {"--httpUserAgent"}, description = "Override User-Agent in request headers")
  String httpUserAgent = null;

  @Parameter(names = {"--httpConnectTimeout"}, description = "Connect timeout in milliseconds (default: 5000)")
  Integer httpConnectTimeout = 5000;

  @Parameter(names = {"--httpRequestTimeout"}, description = "Request timeout in milliseconds (default: 10000)")
  Integer httpRequestTimeout = 10000;

  @Parameter(names = {"--httpMaxConnTotal"}, description = "Max connections to keep open (default: 200)")
  Integer httpMaxConnTotal = 200;

  @Parameter(names = {"--httpMaxConnPerRoute"}, description = "Max connections per route to keep open (default: 100)")
  Integer httpMaxConnPerRoute = 100;

  @Parameter(names = {"--httpAutoRetries"}, description = "Number of times to retry http requests before queueing, set to 0 to disable (default: 3)")
  Integer httpAutoRetries = 3;

  @Parameter(names = {"--preprocessorConfigFile"}, description = "Optional YAML file with additional configuration options for filtering and pre-processing points")
  String preprocessorConfigFile = null;

  @Parameter(names = {"--dataBackfillCutoffHours"}, description = "The cut-off point for what is considered a valid timestamp for back-dated points. Default is 8760 (1 year)")
  int dataBackfillCutoffHours = 8760;

  @Parameter(names = {"--dataPrefillCutoffHours"}, description = "The cut-off point for what is considered a valid timestamp for pre-dated points. Default is 24 (1 day)")
  int dataPrefillCutoffHours = 24;

  @Parameter(names = {"--authMethod"}, converter = TokenValidationMethodConverter.class,
      description = "Authenticate all incoming HTTP requests and disables TCP streams when set to a value " +
          "other than NONE. Allowed values are: NONE, STATIC_TOKEN, HTTP_GET, OAUTH2. Default: NONE")
  TokenValidationMethod authMethod = TokenValidationMethod.NONE;

  @Parameter(names = {"--authTokenIntrospectionServiceUrl"}, description = "URL for the token introspection endpoint " +
      "used to validate tokens for incoming HTTP requests. Required for authMethod = OAUTH2 (endpoint must be " +
      "RFC7662-compliant) and authMethod = HTTP_GET (use {{token}} placeholder in the URL to pass token to the " +
      "service, endpoint must return any 2xx status for valid tokens, any other response code is a fail)")
  String authTokenIntrospectionServiceUrl = null;

  @Parameter(names = {"--authTokenIntrospectionAuthorizationHeader"}, description = "Optional credentials for use " +
      "with the token introspection endpoint.")
  String authTokenIntrospectionAuthorizationHeader = null;

  @Parameter(names = {"--authResponseRefreshInterval"}, description = "Cache TTL (in seconds) for token validation " +
      "results (re-authenticate when expired). Default: 600 seconds")
  int authResponseRefreshInterval = 600;

  @Parameter(names = {"--authResponseMaxTtl"}, description = "Maximum allowed cache TTL (in seconds) for token " +
      "validation results when token introspection service is unavailable. Default: 86400 seconds (1 day)")
  int authResponseMaxTtl = 86400;

  @Parameter(names = {"--authStaticToken"}, description = "Static token that is considered valid " +
      "for all incoming HTTP requests. Required when authMethod = STATIC_TOKEN.")
  String authStaticToken = null;

  @Parameter(names = {"--adminApiListenerPort"}, description = "Enables admin port to control " +
      "healthcheck status per port. Default: none")
  Integer adminApiListenerPort = 0;

  @Parameter(names = {"--adminApiRemoteIpAllowRegex"}, description = "Remote IPs must match " +
      "this regex to access admin API")
  String adminApiRemoteIpAllowRegex = null;

  @Parameter(names = {"--httpHealthCheckPorts"}, description = "Comma-delimited list of ports " +
      "to function as standalone healthchecks. May be used independently of " +
      "--httpHealthCheckAllPorts parameter. Default: none")
  String httpHealthCheckPorts = null;

  @Parameter(names = {"--httpHealthCheckAllPorts"}, description = "When true, all listeners that " +
      "support HTTP protocol also respond to healthcheck requests. May be used independently of " +
      "--httpHealthCheckPorts parameter. Default: false", arity = 1)
  boolean httpHealthCheckAllPorts = false;

  @Parameter(names = {"--httpHealthCheckPath"}, description = "Healthcheck's path, for example, " +
      "'/health'. Default: '/'")
  String httpHealthCheckPath = "/";

  @Parameter(names = {"--httpHealthCheckResponseContentType"}, description = "Optional " +
      "Content-Type to use in healthcheck response, for example, 'application/json'. Default: none")
  String httpHealthCheckResponseContentType = null;

  @Parameter(names = {"--httpHealthCheckPassStatusCode"}, description = "HTTP status code for " +
      "'pass' health checks. Default: 200")
  int httpHealthCheckPassStatusCode = 200;

  @Parameter(names = {"--httpHealthCheckPassResponseBody"}, description = "Optional response " +
      "body to return with 'pass' health checks. Default: none")
  String httpHealthCheckPassResponseBody = null;

  @Parameter(names = {"--httpHealthCheckFailStatusCode"}, description = "HTTP status code for " +
      "'fail' health checks. Default: 503")
  int httpHealthCheckFailStatusCode = 503;

  @Parameter(names = {"--httpHealthCheckFailResponseBody"}, description = "Optional response " +
      "body to return with 'fail' health checks. Default: none")
  String httpHealthCheckFailResponseBody = null;

  @Parameter(names = {"--deltaCountersAggregationIntervalSeconds"},
      description = "Delay time for delta counter reporter. Defaults to 30 seconds.")
  long deltaCountersAggregationIntervalSeconds = 30;

  @Parameter(names = {"--deltaCountersAggregationListenerPorts"},
      description = "Comma-separated list of ports to listen on Wavefront-formatted delta " +
          "counters. Helps reduce outbound point rate by pre-aggregating delta counters at proxy." +
          " Defaults: none")
  String deltaCountersAggregationListenerPorts = "";

  @Parameter(names = {"--privateCertPath"},
          description = "TLS certificate path to use for securing all the ports. " +
                  "X.509 certificate chain file in PEM format.")
  protected String privateCertPath = "";

  @Parameter(names = {"--privateKeyPath"},
          description = "TLS private key path to use for securing all the ports. " +
                  "PKCS#8 private key file in PEM format.")
  protected String privateKeyPath = "";

  @Parameter(names = {"--tlsPorts"},
          description = "Comma-separated list of ports to be secured using TLS. " +
                  "All ports will be secured when * specified.")
  protected String tlsPorts = "";

  @Parameter(names = {"--trafficShaping"}, description = "Enables intelligent traffic shaping " +
      "based on received rate over last 5 minutes. Default: disabled", arity = 1)
  protected boolean trafficShaping = false;

  @Parameter(names = {"--trafficShapingWindowSeconds"}, description = "Sets the width " +
      "(in seconds) for the sliding time window which would be used to calculate received " +
      "traffic rate. Default: 600 (10 minutes)")
  protected Integer trafficShapingWindowSeconds = 600;

  @Parameter(names = {"--trafficShapingHeadroom"}, description = "Sets the headroom multiplier " +
      " to use for traffic shaping when there's backlog. Default: 1.15 (15% headroom)")
  protected double trafficShapingHeadroom = 1.15;

  @Parameter()
  List<String> unparsed_params;

  TimeProvider timeProvider = System::currentTimeMillis;

  public boolean isHelp() {
    return help;
  }

  public boolean isVersion() {
    return version;
  }

  public String getPrefix() {
    return prefix;
  }

  public String getToken() {
    return token;
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

  public Integer getFlushThreads() {
    return flushThreads;
  }

  public int getFlushThreadsSourceTags() {
    return flushThreadsSourceTags;
  }

  public int getFlushThreadsEvents() {
    return flushThreadsEvents;
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

  public int getPushRateLimitMaxBurstSeconds() {
    return pushRateLimitMaxBurstSeconds;
  }

  public int getPushMemoryBufferLimit() {
    return pushMemoryBufferLimit;
  }

  public Integer getPushBlockedSamples() {
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

  public String getPushListenerPorts() {
    return pushListenerPorts;
  }

  public Integer getPushListenerMaxReceivedLength() {
    return pushListenerMaxReceivedLength;
  }

  public Integer getPushListenerHttpBufferSize() {
    return pushListenerHttpBufferSize;
  }

  public Integer getTraceListenerMaxReceivedLength() {
    return traceListenerMaxReceivedLength;
  }

  public Integer getTraceListenerHttpBufferSize() {
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

  public Long getHistogramAccumulatorResolveInterval() {
    return histogramAccumulatorResolveInterval;
  }

  public Long getHistogramAccumulatorFlushInterval() {
    return histogramAccumulatorFlushInterval;
  }

  public Integer getHistogramAccumulatorFlushMaxBatchSize() {
    return histogramAccumulatorFlushMaxBatchSize;
  }

  public Integer getHistogramMaxReceivedLength() {
    return histogramMaxReceivedLength;
  }

  public Integer getHistogramHttpBufferSize() {
    return histogramHttpBufferSize;
  }

  public String getHistogramMinuteListenerPorts() {
    return histogramMinuteListenerPorts;
  }

  public Integer getHistogramMinuteFlushSecs() {
    return histogramMinuteFlushSecs;
  }

  public Short getHistogramMinuteCompression() {
    return histogramMinuteCompression;
  }

  public Integer getHistogramMinuteAvgKeyBytes() {
    return histogramMinuteAvgKeyBytes;
  }

  public Integer getHistogramMinuteAvgDigestBytes() {
    return histogramMinuteAvgDigestBytes;
  }

  public Long getHistogramMinuteAccumulatorSize() {
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

  public Integer getHistogramHourFlushSecs() {
    return histogramHourFlushSecs;
  }

  public Short getHistogramHourCompression() {
    return histogramHourCompression;
  }

  public Integer getHistogramHourAvgKeyBytes() {
    return histogramHourAvgKeyBytes;
  }

  public Integer getHistogramHourAvgDigestBytes() {
    return histogramHourAvgDigestBytes;
  }

  public Long getHistogramHourAccumulatorSize() {
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

  public Integer getHistogramDayFlushSecs() {
    return histogramDayFlushSecs;
  }

  public Short getHistogramDayCompression() {
    return histogramDayCompression;
  }

  public Integer getHistogramDayAvgKeyBytes() {
    return histogramDayAvgKeyBytes;
  }

  public Integer getHistogramDayAvgDigestBytes() {
    return histogramDayAvgDigestBytes;
  }

  public Long getHistogramDayAccumulatorSize() {
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

  public Integer getHistogramDistFlushSecs() {
    return histogramDistFlushSecs;
  }

  public Short getHistogramDistCompression() {
    return histogramDistCompression;
  }

  public Integer getHistogramDistAvgKeyBytes() {
    return histogramDistAvgKeyBytes;
  }

  public Integer getHistogramDistAvgDigestBytes() {
    return histogramDistAvgDigestBytes;
  }

  public Long getHistogramDistAccumulatorSize() {
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

  public Integer getFilebeatPort() {
    return filebeatPort;
  }

  public Integer getRawLogsPort() {
    return rawLogsPort;
  }

  public Integer getRawLogsMaxReceivedLength() {
    return rawLogsMaxReceivedLength;
  }

  public Integer getRawLogsHttpBufferSize() {
    return rawLogsHttpBufferSize;
  }

  public String getLogsIngestionConfigFile() {
    return logsIngestionConfigFile;
  }

  public String getHostname() {
    return hostname;
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

  public Integer getTraceSamplingDuration() {
    return traceSamplingDuration;
  }

  public Set<String> getTraceDerivedCustomTagKeys() {
    Set<String> customTagKeys = new HashSet<>(Splitter.on(",").trimResults().omitEmptyStrings().
            splitToList(ObjectUtils.firstNonNull(traceDerivedCustomTagKeys, "")));
    customTagKeys.add(SPAN_KIND.getKey());  // add span.kind tag by default
    return customTagKeys;
  }

  public boolean isTraceAlwaysSampleErrors() {
    return traceAlwaysSampleErrors;
  }

  public String getPushRelayListenerPorts() {
    return pushRelayListenerPorts;
  }

  public boolean isPushRelayHistogramAggregator() {
    return pushRelayHistogramAggregator;
  }

  public Long getPushRelayHistogramAggregatorAccumulatorSize() {
    return pushRelayHistogramAggregatorAccumulatorSize;
  }

  public Integer getPushRelayHistogramAggregatorFlushSecs() {
    return pushRelayHistogramAggregatorFlushSecs;
  }

  public Short getPushRelayHistogramAggregatorCompression() {
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
    Splitter.on(",").trimResults().omitEmptyStrings().split(customSourceTags).forEach(x -> {
      if (!tagSet.add(x)) {
        logger.warning("Duplicate tag " + x + " specified in customSourceTags config setting");
      }
    });
    return new ArrayList<>(tagSet);
  }

  public Map<String, String> getAgentMetricsPointTags() {
    //noinspection UnstableApiUsage
    return agentMetricsPointTags == null ? Collections.emptyMap() :
        Splitter.on(",").trimResults().omitEmptyStrings().
            withKeyValueSeparator("=").split(agentMetricsPointTags);
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

  public Integer getSoLingerTime() {
    return soLingerTime;
  }

  public String getProxyHost() {
    return proxyHost;
  }

  public Integer getProxyPort() {
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

  public Integer getHttpConnectTimeout() {
    return httpConnectTimeout;
  }

  public Integer getHttpRequestTimeout() {
    return httpRequestTimeout;
  }

  public Integer getHttpMaxConnTotal() {
    return httpMaxConnTotal;
  }

  public Integer getHttpMaxConnPerRoute() {
    return httpMaxConnPerRoute;
  }

  public Integer getHttpAutoRetries() {
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

  public Integer getAdminApiListenerPort() {
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

  public Integer getTrafficShapingWindowSeconds() {
    return trafficShapingWindowSeconds;
  }

  public double getTrafficShapingHeadroom() {
    return trafficShapingHeadroom;
  }

  @Override
  public void verifyAndInit() {
    if (unparsed_params != null) {
      logger.info("Unparsed arguments: " + Joiner.on(", ").join(unparsed_params));
    }

    ReportableConfig config;
    // If they've specified a push configuration file, override the command line values
    try {
      if (pushConfigFile != null) {
        config = new ReportableConfig(pushConfigFile);
      } else {
        config = new ReportableConfig(); // dummy config
      }
      prefix = Strings.emptyToNull(config.getString("prefix", prefix));
      // don't track token in proxy config metrics
      token = ObjectUtils.firstNonNull(config.getRawProperty("token", token), "undefined").trim();
      server = config.getString("server", server);
      hostname = config.getString("hostname", hostname);
      idFile = config.getString("idFile", idFile);
      pushRateLimit = config.getInteger("pushRateLimit", pushRateLimit);
      pushRateLimitHistograms = config.getInteger("pushRateLimitHistograms",
          pushRateLimitHistograms);
      pushRateLimitSourceTags = config.getDouble("pushRateLimitSourceTags",
          pushRateLimitSourceTags);
      pushRateLimitSpans = config.getInteger("pushRateLimitSpans", pushRateLimitSpans);
      pushRateLimitSpanLogs = config.getInteger("pushRateLimitSpanLogs", pushRateLimitSpanLogs);
      pushRateLimitEvents = config.getDouble("pushRateLimitEvents", pushRateLimitEvents);
      pushRateLimitMaxBurstSeconds = config.getInteger("pushRateLimitMaxBurstSeconds",
          pushRateLimitMaxBurstSeconds);
      pushBlockedSamples = config.getInteger("pushBlockedSamples", pushBlockedSamples);
      blockedPointsLoggerName = config.getString("blockedPointsLoggerName",
          blockedPointsLoggerName);
      blockedHistogramsLoggerName = config.getString("blockedHistogramsLoggerName",
          blockedHistogramsLoggerName);
      blockedSpansLoggerName = config.getString("blockedSpansLoggerName",
          blockedSpansLoggerName);
      pushListenerPorts = config.getString("pushListenerPorts", pushListenerPorts);
      pushListenerMaxReceivedLength = config.getInteger("pushListenerMaxReceivedLength",
          pushListenerMaxReceivedLength);
      pushListenerHttpBufferSize = config.getInteger("pushListenerHttpBufferSize",
          pushListenerHttpBufferSize);
      traceListenerMaxReceivedLength = config.getInteger("traceListenerMaxReceivedLength",
          traceListenerMaxReceivedLength);
      traceListenerHttpBufferSize = config.getInteger("traceListenerHttpBufferSize",
          traceListenerHttpBufferSize);
      listenerIdleConnectionTimeout = config.getInteger("listenerIdleConnectionTimeout",
          listenerIdleConnectionTimeout);
      memGuardFlushThreshold = config.getInteger("memGuardFlushThreshold", memGuardFlushThreshold);

      // Histogram: global settings
      histogramPassthroughRecompression = config.getBoolean("histogramPassthroughRecompression",
          histogramPassthroughRecompression);
      histogramStateDirectory = config.getString("histogramStateDirectory",
          histogramStateDirectory);
      histogramAccumulatorResolveInterval = config.getLong("histogramAccumulatorResolveInterval",
          histogramAccumulatorResolveInterval);
      histogramAccumulatorFlushInterval = config.getLong("histogramAccumulatorFlushInterval",
          histogramAccumulatorFlushInterval);
      histogramAccumulatorFlushMaxBatchSize =
          config.getInteger("histogramAccumulatorFlushMaxBatchSize",
              histogramAccumulatorFlushMaxBatchSize);
      histogramMaxReceivedLength = config.getInteger("histogramMaxReceivedLength",
          histogramMaxReceivedLength);
      histogramHttpBufferSize = config.getInteger("histogramHttpBufferSize",
          histogramHttpBufferSize);

      deltaCountersAggregationListenerPorts =
          config.getString("deltaCountersAggregationListenerPorts",
              deltaCountersAggregationListenerPorts);
      deltaCountersAggregationIntervalSeconds =
          config.getLong("deltaCountersAggregationIntervalSeconds",
              deltaCountersAggregationIntervalSeconds);

      customTracingListenerPorts =
          config.getString("customTracingListenerPorts", customTracingListenerPorts);

      // Histogram: deprecated settings - fall back for backwards compatibility
      if (config.isDefined("avgHistogramKeyBytes")) {
        histogramMinuteAvgKeyBytes = histogramHourAvgKeyBytes = histogramDayAvgKeyBytes =
            histogramDistAvgKeyBytes = config.getInteger("avgHistogramKeyBytes", 150);
      }
      if (config.isDefined("avgHistogramDigestBytes")) {
        histogramMinuteAvgDigestBytes = histogramHourAvgDigestBytes = histogramDayAvgDigestBytes =
            histogramDistAvgDigestBytes = config.getInteger("avgHistogramDigestBytes", 500);
      }
      if (config.isDefined("histogramAccumulatorSize")) {
        histogramMinuteAccumulatorSize = histogramHourAccumulatorSize =
            histogramDayAccumulatorSize = histogramDistAccumulatorSize = config.getLong(
                "histogramAccumulatorSize", 100000);
      }
      if (config.isDefined("histogramCompression")) {
        histogramMinuteCompression = histogramHourCompression = histogramDayCompression =
            histogramDistCompression = config.getNumber("histogramCompression", null, 20, 1000).
                shortValue();
      }
      if (config.isDefined("persistAccumulator")) {
        histogramMinuteAccumulatorPersisted = histogramHourAccumulatorPersisted =
            histogramDayAccumulatorPersisted = histogramDistAccumulatorPersisted =
                config.getBoolean("persistAccumulator", false);
      }

      // Histogram: minute accumulator settings
      histogramMinuteListenerPorts = config.getString("histogramMinuteListenerPorts",
          histogramMinuteListenerPorts);
      histogramMinuteFlushSecs = config.getInteger("histogramMinuteFlushSecs",
          histogramMinuteFlushSecs);
      histogramMinuteCompression = config.getNumber("histogramMinuteCompression",
          histogramMinuteCompression, 20, 1000).shortValue();
      histogramMinuteAvgKeyBytes = config.getInteger("histogramMinuteAvgKeyBytes",
          histogramMinuteAvgKeyBytes);
      histogramMinuteAvgDigestBytes = 32 + histogramMinuteCompression * 7;
      histogramMinuteAvgDigestBytes = config.getInteger("histogramMinuteAvgDigestBytes",
          histogramMinuteAvgDigestBytes);
      histogramMinuteAccumulatorSize = config.getLong("histogramMinuteAccumulatorSize",
          histogramMinuteAccumulatorSize);
      histogramMinuteAccumulatorPersisted = config.getBoolean("histogramMinuteAccumulatorPersisted",
          histogramMinuteAccumulatorPersisted);
      histogramMinuteMemoryCache = config.getBoolean("histogramMinuteMemoryCache",
          histogramMinuteMemoryCache);

      // Histogram: hour accumulator settings
      histogramHourListenerPorts = config.getString("histogramHourListenerPorts",
          histogramHourListenerPorts);
      histogramHourFlushSecs = config.getInteger("histogramHourFlushSecs", histogramHourFlushSecs);
      histogramHourCompression = config.getNumber("histogramHourCompression",
          histogramHourCompression, 20, 1000).shortValue();
      histogramHourAvgKeyBytes = config.getInteger("histogramHourAvgKeyBytes",
          histogramHourAvgKeyBytes);
      histogramHourAvgDigestBytes = 32 + histogramHourCompression * 7;
      histogramHourAvgDigestBytes = config.getInteger("histogramHourAvgDigestBytes",
          histogramHourAvgDigestBytes);
      histogramHourAccumulatorSize = config.getLong("histogramHourAccumulatorSize",
          histogramHourAccumulatorSize);
      histogramHourAccumulatorPersisted = config.getBoolean("histogramHourAccumulatorPersisted",
          histogramHourAccumulatorPersisted);
      histogramHourMemoryCache = config.getBoolean("histogramHourMemoryCache",
          histogramHourMemoryCache);

      // Histogram: day accumulator settings
      histogramDayListenerPorts = config.getString("histogramDayListenerPorts",
          histogramDayListenerPorts);
      histogramDayFlushSecs = config.getInteger("histogramDayFlushSecs", histogramDayFlushSecs);
      histogramDayCompression = config.getNumber("histogramDayCompression",
          histogramDayCompression, 20, 1000).shortValue();
      histogramDayAvgKeyBytes = config.getInteger("histogramDayAvgKeyBytes",
          histogramDayAvgKeyBytes);
      histogramDayAvgDigestBytes = 32 + histogramDayCompression * 7;
      histogramDayAvgDigestBytes = config.getInteger("histogramDayAvgDigestBytes",
          histogramDayAvgDigestBytes);
      histogramDayAccumulatorSize = config.getLong("histogramDayAccumulatorSize",
          histogramDayAccumulatorSize);
      histogramDayAccumulatorPersisted = config.getBoolean("histogramDayAccumulatorPersisted",
          histogramDayAccumulatorPersisted);
      histogramDayMemoryCache = config.getBoolean("histogramDayMemoryCache",
          histogramDayMemoryCache);

      // Histogram: dist accumulator settings
      histogramDistListenerPorts = config.getString("histogramDistListenerPorts",
          histogramDistListenerPorts);
      histogramDistFlushSecs = config.getInteger("histogramDistFlushSecs", histogramDistFlushSecs);
      histogramDistCompression = config.getNumber("histogramDistCompression",
          histogramDistCompression, 20, 1000).shortValue();
      histogramDistAvgKeyBytes = config.getInteger("histogramDistAvgKeyBytes",
          histogramDistAvgKeyBytes);
      histogramDistAvgDigestBytes = 32 + histogramDistCompression * 7;
      histogramDistAvgDigestBytes = config.getInteger("histogramDistAvgDigestBytes",
          histogramDistAvgDigestBytes);
      histogramDistAccumulatorSize = config.getLong("histogramDistAccumulatorSize",
          histogramDistAccumulatorSize);
      histogramDistAccumulatorPersisted = config.getBoolean("histogramDistAccumulatorPersisted",
          histogramDistAccumulatorPersisted);
      histogramDistMemoryCache = config.getBoolean("histogramDistMemoryCache",
          histogramDistMemoryCache);

      exportQueuePorts = config.getString("exportQueuePorts", exportQueuePorts);
      exportQueueOutputFile = config.getString("exportQueueOutputFile", exportQueueOutputFile);
      exportQueueRetainData = config.getBoolean("exportQueueRetainData", exportQueueRetainData);
      useNoopSender = config.getBoolean("useNoopSender", useNoopSender);
      flushThreads = config.getInteger("flushThreads", flushThreads);
      flushThreadsEvents = config.getInteger("flushThreadsEvents", flushThreadsEvents);
      flushThreadsSourceTags = config.getInteger("flushThreadsSourceTags", flushThreadsSourceTags);
      jsonListenerPorts = config.getString("jsonListenerPorts", jsonListenerPorts);
      writeHttpJsonListenerPorts = config.getString("writeHttpJsonListenerPorts",
          writeHttpJsonListenerPorts);
      dataDogJsonPorts = config.getString("dataDogJsonPorts", dataDogJsonPorts);
      dataDogRequestRelayTarget = config.getString("dataDogRequestRelayTarget",
          dataDogRequestRelayTarget);
      dataDogRequestRelayAsyncThreads = config.getInteger("dataDogRequestRelayAsyncThreads",
          dataDogRequestRelayAsyncThreads);
      dataDogRequestRelaySyncMode = config.getBoolean("dataDogRequestRelaySyncMode",
          dataDogRequestRelaySyncMode);
      dataDogProcessSystemMetrics = config.getBoolean("dataDogProcessSystemMetrics",
          dataDogProcessSystemMetrics);
      dataDogProcessServiceChecks = config.getBoolean("dataDogProcessServiceChecks",
          dataDogProcessServiceChecks);
      graphitePorts = config.getString("graphitePorts", graphitePorts);
      graphiteFormat = config.getString("graphiteFormat", graphiteFormat);
      graphiteFieldsToRemove = config.getString("graphiteFieldsToRemove", graphiteFieldsToRemove);
      graphiteDelimiters = config.getString("graphiteDelimiters", graphiteDelimiters);
      allowRegex = config.getString("allowRegex", config.getString("whitelistRegex", allowRegex));
      blockRegex = config.getString("blockRegex", config.getString("blacklistRegex", blockRegex));
      opentsdbPorts = config.getString("opentsdbPorts", opentsdbPorts);
      opentsdbAllowRegex = config.getString("opentsdbAllowRegex",
          config.getString("opentsdbWhitelistRegex", opentsdbAllowRegex));
      opentsdbBlockRegex = config.getString("opentsdbBlockRegex",
          config.getString("opentsdbBlacklistRegex", opentsdbBlockRegex));
      proxyHost = config.getString("proxyHost", proxyHost);
      proxyPort = config.getInteger("proxyPort", proxyPort);
      proxyPassword = config.getString("proxyPassword", proxyPassword, s -> "<removed>");
      proxyUser = config.getString("proxyUser", proxyUser);
      httpUserAgent = config.getString("httpUserAgent", httpUserAgent);
      httpConnectTimeout = config.getInteger("httpConnectTimeout", httpConnectTimeout);
      httpRequestTimeout = config.getInteger("httpRequestTimeout", httpRequestTimeout);
      httpMaxConnTotal = Math.min(200, config.getInteger("httpMaxConnTotal", httpMaxConnTotal));
      httpMaxConnPerRoute = Math.min(100, config.getInteger("httpMaxConnPerRoute",
          httpMaxConnPerRoute));
      httpAutoRetries = config.getInteger("httpAutoRetries", httpAutoRetries);
      gzipCompression = config.getBoolean("gzipCompression", gzipCompression);
      gzipCompressionLevel = config.getNumber("gzipCompressionLevel", gzipCompressionLevel, 1, 9).
          intValue();
      soLingerTime = config.getInteger("soLingerTime", soLingerTime);
      splitPushWhenRateLimited = config.getBoolean("splitPushWhenRateLimited",
          splitPushWhenRateLimited);
      customSourceTags = config.getString("customSourceTags", customSourceTags);
      agentMetricsPointTags = config.getString("agentMetricsPointTags", agentMetricsPointTags);
      ephemeral = config.getBoolean("ephemeral", ephemeral);
      disableRdnsLookup = config.getBoolean("disableRdnsLookup", disableRdnsLookup);
      picklePorts = config.getString("picklePorts", picklePorts);
      traceListenerPorts = config.getString("traceListenerPorts", traceListenerPorts);
      traceJaegerListenerPorts = config.getString("traceJaegerListenerPorts",
          traceJaegerListenerPorts);
      traceJaegerHttpListenerPorts = config.getString("traceJaegerHttpListenerPorts",
          traceJaegerHttpListenerPorts);
      traceJaegerGrpcListenerPorts = config.getString("traceJaegerGrpcListenerPorts",
          traceJaegerGrpcListenerPorts);
      traceJaegerApplicationName = config.getString("traceJaegerApplicationName",
          traceJaegerApplicationName);
      traceZipkinListenerPorts = config.getString("traceZipkinListenerPorts",
          traceZipkinListenerPorts);
      traceZipkinApplicationName = config.getString("traceZipkinApplicationName",
          traceZipkinApplicationName);
      customTracingListenerPorts =
          config.getString("customTracingListenerPorts", customTracingListenerPorts);
      customTracingApplicationName =
          config.getString("customTracingApplicationName", customTracingApplicationName);
      customTracingServiceName =
          config.getString("customTracingServiceName", customTracingServiceName);
      traceSamplingRate = config.getDouble("traceSamplingRate", traceSamplingRate);
      traceSamplingDuration = config.getInteger("traceSamplingDuration", traceSamplingDuration);
      traceDerivedCustomTagKeys = config.getString("traceDerivedCustomTagKeys",
          traceDerivedCustomTagKeys);
      traceAlwaysSampleErrors = config.getBoolean("traceAlwaysSampleErrors",
          traceAlwaysSampleErrors);
      pushRelayListenerPorts = config.getString("pushRelayListenerPorts", pushRelayListenerPorts);
      pushRelayHistogramAggregator = config.getBoolean("pushRelayHistogramAggregator",
          pushRelayHistogramAggregator);
      pushRelayHistogramAggregatorAccumulatorSize =
          config.getLong("pushRelayHistogramAggregatorAccumulatorSize",
              pushRelayHistogramAggregatorAccumulatorSize);
      pushRelayHistogramAggregatorFlushSecs = config.getInteger(
          "pushRelayHistogramAggregatorFlushSecs", pushRelayHistogramAggregatorFlushSecs);
      pushRelayHistogramAggregatorCompression =
          config.getNumber("pushRelayHistogramAggregatorCompression",
              pushRelayHistogramAggregatorCompression).shortValue();
      bufferFile = config.getString("buffer", bufferFile);
      taskQueueLevel = TaskQueueLevel.fromString(config.getString("taskQueueStrategy",
          taskQueueLevel.toString()));
      purgeBuffer = config.getBoolean("purgeBuffer", purgeBuffer);
      preprocessorConfigFile = config.getString("preprocessorConfigFile", preprocessorConfigFile);
      dataBackfillCutoffHours = config.getInteger("dataBackfillCutoffHours", dataBackfillCutoffHours);
      dataPrefillCutoffHours = config.getInteger("dataPrefillCutoffHours", dataPrefillCutoffHours);
      filebeatPort = config.getInteger("filebeatPort", filebeatPort);
      rawLogsPort = config.getInteger("rawLogsPort", rawLogsPort);
      rawLogsMaxReceivedLength = config.getInteger("rawLogsMaxReceivedLength",
          rawLogsMaxReceivedLength);
      rawLogsHttpBufferSize = config.getInteger("rawLogsHttpBufferSize", rawLogsHttpBufferSize);
      logsIngestionConfigFile = config.getString("logsIngestionConfigFile", logsIngestionConfigFile);

      sqsQueueBuffer = config.getBoolean("sqsBuffer", sqsQueueBuffer);
      sqsQueueNameTemplate = config.getString("sqsQueueNameTemplate", sqsQueueNameTemplate);
      sqsQueueRegion = config.getString("sqsQueueRegion", sqsQueueRegion);
      sqsQueueIdentifier = config.getString("sqsQueueIdentifier", sqsQueueIdentifier);

      // auth settings
      authMethod = TokenValidationMethod.fromString(config.getString("authMethod",
          authMethod.toString()));
      authTokenIntrospectionServiceUrl = config.getString("authTokenIntrospectionServiceUrl",
          authTokenIntrospectionServiceUrl);
      authTokenIntrospectionAuthorizationHeader = config.getString(
          "authTokenIntrospectionAuthorizationHeader", authTokenIntrospectionAuthorizationHeader);
      authResponseRefreshInterval = config.getInteger("authResponseRefreshInterval",
          authResponseRefreshInterval);
      authResponseMaxTtl = config.getInteger("authResponseMaxTtl", authResponseMaxTtl);
      authStaticToken = config.getString("authStaticToken", authStaticToken);

      // health check / admin API settings
      adminApiListenerPort = config.getInteger("adminApiListenerPort", adminApiListenerPort);
      adminApiRemoteIpAllowRegex = config.getString("adminApiRemoteIpWhitelistRegex",
          adminApiRemoteIpAllowRegex);
      httpHealthCheckPorts = config.getString("httpHealthCheckPorts", httpHealthCheckPorts);
      httpHealthCheckAllPorts = config.getBoolean("httpHealthCheckAllPorts", false);
      httpHealthCheckPath = config.getString("httpHealthCheckPath", httpHealthCheckPath);
      httpHealthCheckResponseContentType = config.getString("httpHealthCheckResponseContentType",
          httpHealthCheckResponseContentType);
      httpHealthCheckPassStatusCode = config.getInteger("httpHealthCheckPassStatusCode",
          httpHealthCheckPassStatusCode);
      httpHealthCheckPassResponseBody = config.getString("httpHealthCheckPassResponseBody",
          httpHealthCheckPassResponseBody);
      httpHealthCheckFailStatusCode = config.getInteger("httpHealthCheckFailStatusCode",
          httpHealthCheckFailStatusCode);
      httpHealthCheckFailResponseBody = config.getString("httpHealthCheckFailResponseBody",
          httpHealthCheckFailResponseBody);

      // TLS configurations
      privateCertPath = config.getString("privateCertPath", privateCertPath);
      privateKeyPath = config.getString("privateKeyPath", privateKeyPath);
      tlsPorts = config.getString("tlsPorts", tlsPorts);

      // Traffic shaping config
      trafficShaping = config.getBoolean("trafficShaping", trafficShaping);
      trafficShapingWindowSeconds = config.getInteger("trafficShapingWindowSeconds",
          trafficShapingWindowSeconds);
      trafficShapingHeadroom = config.getDouble("trafficShapingHeadroom", trafficShapingHeadroom);

      // clamp values for pushFlushMaxPoints/etc between min split size
      // (or 1 in case of source tags and events) and default batch size.
      // also make sure it is never higher than the configured rate limit.
      pushFlushMaxPoints = Math.max(Math.min(Math.min(config.getInteger("pushFlushMaxPoints",
          pushFlushMaxPoints), DEFAULT_BATCH_SIZE), (int) pushRateLimit),
          DEFAULT_MIN_SPLIT_BATCH_SIZE);
      pushFlushMaxHistograms = Math.max(Math.min(Math.min(config.getInteger(
          "pushFlushMaxHistograms", pushFlushMaxHistograms), DEFAULT_BATCH_SIZE_HISTOGRAMS),
          (int) pushRateLimitHistograms), DEFAULT_MIN_SPLIT_BATCH_SIZE);
      pushFlushMaxSourceTags = Math.max(Math.min(Math.min(config.getInteger(
          "pushFlushMaxSourceTags", pushFlushMaxSourceTags),
          DEFAULT_BATCH_SIZE_SOURCE_TAGS), (int) pushRateLimitSourceTags), 1);
      pushFlushMaxSpans = Math.max(Math.min(Math.min(config.getInteger("pushFlushMaxSpans",
          pushFlushMaxSpans), DEFAULT_BATCH_SIZE_SPANS), (int) pushRateLimitSpans),
          DEFAULT_MIN_SPLIT_BATCH_SIZE);
      pushFlushMaxSpanLogs = Math.max(Math.min(Math.min(config.getInteger("pushFlushMaxSpanLogs",
          pushFlushMaxSpanLogs), DEFAULT_BATCH_SIZE_SPAN_LOGS),
          (int) pushRateLimitSpanLogs), DEFAULT_MIN_SPLIT_BATCH_SIZE);
      pushFlushMaxEvents = Math.min(Math.min(Math.max(config.getInteger("pushFlushMaxEvents",
          pushFlushMaxEvents), 1), DEFAULT_BATCH_SIZE_EVENTS), (int) (pushRateLimitEvents + 1));

      /*
        default value for pushMemoryBufferLimit is 16 * pushFlushMaxPoints, but no more than 25% of
        available heap memory. 25% is chosen heuristically as a safe number for scenarios with
        limited system resources (4 CPU cores or less, heap size less than 4GB) to prevent OOM.
        this is a conservative estimate, budgeting 200 characters (400 bytes) per per point line.
        Also, it shouldn't be less than 1 batch size (pushFlushMaxPoints).
       */
      int listeningPorts = Iterables.size(Splitter.on(",").omitEmptyStrings().trimResults().
          split(pushListenerPorts));
      long calculatedMemoryBufferLimit = Math.max(Math.min(16 * pushFlushMaxPoints,
          Runtime.getRuntime().maxMemory() / Math.max(0, listeningPorts) / 4 / flushThreads / 400),
          pushFlushMaxPoints);
      logger.fine("Calculated pushMemoryBufferLimit: " + calculatedMemoryBufferLimit);
      pushMemoryBufferLimit = Math.max(config.getInteger("pushMemoryBufferLimit",
          pushMemoryBufferLimit), pushFlushMaxPoints);
      logger.fine("Configured pushMemoryBufferLimit: " + pushMemoryBufferLimit);
      pushFlushInterval = config.getInteger("pushFlushInterval", pushFlushInterval);
      retryBackoffBaseSeconds = Math.max(Math.min(config.getDouble("retryBackoffBaseSeconds",
          retryBackoffBaseSeconds), MAX_RETRY_BACKOFF_BASE_SECONDS), 1.0);
    } catch (Throwable exception) {
      logger.severe("Could not load configuration file " + pushConfigFile);
      throw new RuntimeException(exception.getMessage());
    }
    if (httpUserAgent == null) {
      httpUserAgent = "Wavefront-Proxy/" + getBuildVersion();
    }
    if (pushConfigFile != null) {
      logger.info("Loaded configuration file " + pushConfigFile);
    }
  }

  /**
   * Parse commandline arguments into {@link ProxyConfig} object.
   *
   * @param args        arguments to parse
   * @param programName program name (to display help)
   * @return true if proxy should continue, false if proxy should terminate.
   * @throws ParameterException if configuration parsing failed
   */
  public boolean parseArguments(String[] args, String programName)
      throws ParameterException {
    String versionStr = "Wavefront Proxy version " + getBuildVersion();
    JCommander jCommander = JCommander.newBuilder().
        programName(programName).
        addObject(this).
        allowParameterOverwriting(true).
        build();
    jCommander.parse(args);
    if (this.isVersion()) {
      System.out.println(versionStr);
      return false;
    }
    if (this.isHelp()) {
      System.out.println(versionStr);
      jCommander.usage();
      return false;
    }
    return true;
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
}

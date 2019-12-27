package com.wavefront.agent.config;

import com.beust.jcommander.Parameter;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.wavefront.agent.auth.TokenValidationMethod;
import org.apache.commons.lang3.ObjectUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import static com.wavefront.agent.Utils.getLocalHostName;
import static com.wavefront.agent.Utils.getBuildVersion;
import static com.wavefront.agent.data.ProxyRuntimeProperties.DEFAULT_BATCH_SIZE;
import static com.wavefront.agent.data.ProxyRuntimeProperties.DEFAULT_BATCH_SIZE_EVENTS;
import static com.wavefront.agent.data.ProxyRuntimeProperties.DEFAULT_BATCH_SIZE_HISTOGRAMS;
import static com.wavefront.agent.data.ProxyRuntimeProperties.DEFAULT_BATCH_SIZE_SOURCE_TAGS;
import static com.wavefront.agent.data.ProxyRuntimeProperties.DEFAULT_BATCH_SIZE_SPANS;
import static com.wavefront.agent.data.ProxyRuntimeProperties.DEFAULT_BATCH_SIZE_SPAN_LOGS;
import static com.wavefront.agent.data.ProxyRuntimeProperties.DEFAULT_FLUSH_INTERVAL;
import static com.wavefront.agent.data.ProxyRuntimeProperties.DEFAULT_MIN_SPLIT_BATCH_SIZE;
import static com.wavefront.agent.data.ProxyRuntimeProperties.DEFAULT_RETRY_BACKOFF_BASE_SECONDS;
import static com.wavefront.agent.data.ProxyRuntimeProperties.DEFAULT_SPLIT_PUSH_WHEN_RATE_LIMITED;
import static com.wavefront.agent.handlers.RecyclableRateLimiterFactoryImpl.NO_RATE_LIMIT;

/**
 * Proxy configuration (refactored from {@link com.wavefront.agent.AbstractAgent}).
 *
 * @author vasily@wavefront.com
 */
public class ProxyConfig extends Configuration {
  protected static final Logger logger = Logger.getLogger(ProxyConfig.class.getCanonicalName());
  private static final double MAX_RETRY_BACKOFF_BASE_SECONDS = 60.0;
  private static final int GRAPHITE_LISTENING_PORT = 2878;

  @Parameter(names = {"--help"}, help = true)
  private boolean help = false;

  @Parameter(names = {"--version"}, description = "Print version and exit.", order = 0)
  private boolean version = false;

  @Parameter(names = {"-f", "--file"}, description =
      "Proxy configuration file", order = 1)
  private String pushConfigFile = null;

  @Parameter(names = {"-c", "--config"}, description =
      "Local configuration file to use (overrides using the server to obtain a config file)")
  private String configFile = null;

  @Parameter(names = {"-p", "--prefix"}, description =
      "Prefix to prepend to all push metrics before reporting.")
  private String prefix = null;

  @Parameter(names = {"-t", "--token"}, description =
      "Token to auto-register proxy with an account", order = 3)
  private String token = null;

  @Parameter(names = {"--testLogs"}, description = "Run interactive session for crafting logsIngestionConfig.yaml")
  private boolean testLogs = false;

  @Parameter(names = {"-v", "--validationlevel", "--pushValidationLevel"}, description =
      "Validation level for push data (NO_VALIDATION/NUMERIC_ONLY); NUMERIC_ONLY is default")
  private String pushValidationLevel = "NUMERIC_ONLY";

  @Parameter(names = {"-h", "--host"}, description = "Server URL", order = 2)
  private String server = "http://localhost:8080/api/";

  @Parameter(names = {"--buffer"}, description = "File to use for buffering transmissions " +
      "to be retried. Defaults to /var/spool/wavefront-proxy/buffer.", order = 7)
  private String bufferFile = "/var/spool/wavefront-proxy/buffer";

  @Parameter(names = {"--retryThreads"}, description = "Number of threads retrying failed transmissions. Defaults to " +
      "the number of processors (min. 4). Buffer files are maxed out at 2G each so increasing the number of retry " +
      "threads effectively governs the maximum amount of space the proxy will use to buffer points locally", order = 6)
  private Integer retryThreads = Math.min(16, Math.max(4, Runtime.getRuntime().availableProcessors()));

  @Parameter(names = {"--flushThreads"}, description = "Number of threads that flush data to the server. Defaults to" +
      "the number of processors (min. 4). Setting this value too large will result in sending batches that are too " +
      "small to the server and wasting connections. This setting is per listening port.", order = 5)
  private Integer flushThreads = Math.min(16, Math.max(4, Runtime.getRuntime().availableProcessors()));

  @Parameter(names = {"--purgeBuffer"}, description = "Whether to purge the retry buffer on start-up. Defaults to " +
      "false.", arity = 1)
  private boolean purgeBuffer = false;

  @Parameter(names = {"--pushFlushInterval"}, description = "Milliseconds between batches. " +
      "Defaults to 1000 ms")
  private int pushFlushInterval = DEFAULT_FLUSH_INTERVAL;

  @Parameter(names = {"--pushFlushMaxPoints"}, description = "Maximum allowed points " +
      "in a single flush. Defaults: 40000")
  private int pushFlushMaxPoints = DEFAULT_BATCH_SIZE;

  @Parameter(names = {"--pushFlushMaxHistograms"}, description = "Maximum allowed histograms " +
      "in a single flush. Default: 10000")
  private int pushFlushMaxHistograms = DEFAULT_BATCH_SIZE_HISTOGRAMS;

  @Parameter(names = {"--pushFlushMaxSourceTags"}, description = "Maximum allowed source tags " +
      "in a single flush. Default: 50")
  private int pushFlushMaxSourceTags = DEFAULT_BATCH_SIZE_SOURCE_TAGS;

  @Parameter(names = {"--pushFlushMaxSpans"}, description = "Maximum allowed spans " +
      "in a single flush. Default: 5000")
  private int pushFlushMaxSpans = DEFAULT_BATCH_SIZE_SPANS;

  @Parameter(names = {"--pushFlushMaxSpanLogs"}, description = "Maximum allowed span logs " +
      "in a single flush. Default: 1000")
  private int pushFlushMaxSpanLogs = DEFAULT_BATCH_SIZE_SPAN_LOGS;

  @Parameter(names = {"--pushFlushMaxEvents"}, description = "Maximum allowed events " +
      "in a single flush. Default: 50")
  private int pushFlushMaxEvents = DEFAULT_BATCH_SIZE_EVENTS;

  @Parameter(names = {"--pushRateLimit"}, description = "Limit the outgoing point rate at the proxy. Default: " +
      "do not throttle.")
  private Integer pushRateLimit = NO_RATE_LIMIT;

  @Parameter(names = {"--pushRateLimitHistograms"}, description = "Limit the outgoing histogram " +
      "rate at the proxy. Default: do not throttle.")
  private Integer pushRateLimitHistograms = NO_RATE_LIMIT;

  @Parameter(names = {"--pushRateLimitSourceTags"}, description = "Limit the outgoing rate " +
      "for source tags at the proxy. Default: 5 op/s")
  private Double pushRateLimitSourceTags = 5.0d;

  @Parameter(names = {"--pushRateLimitSpans"}, description = "Limit the outgoing tracing spans " +
      "rate at the proxy. Default: do not throttle.")
  private Integer pushRateLimitSpans = NO_RATE_LIMIT;

  @Parameter(names = {"--pushRateLimitSpanLogs"}, description = "Limit the outgoing span logs " +
      "rate at the proxy. Default: do not throttle.")
  private Integer pushRateLimitSpanLogs = NO_RATE_LIMIT;

  @Parameter(names = {"--pushRateLimitEvents"}, description = "Limit the outgoing rate " +
      "for events at the proxy. Default: 5 events/s")
  private Double pushRateLimitEvents = 5.0d;

  @Parameter(names = {"--pushRateLimitMaxBurstSeconds"}, description = "Max number of burst seconds to allow " +
      "when rate limiting to smooth out uneven traffic. Set to 1 when doing data backfills. Default: 10")
  private Integer pushRateLimitMaxBurstSeconds = 10;

  @Parameter(names = {"--pushMemoryBufferLimit"}, description = "Max number of points that can stay in memory buffers" +
      " before spooling to disk. Defaults to 16 * pushFlushMaxPoints, minimum size: pushFlushMaxPoints. Setting this " +
      " value lower than default reduces memory usage but will force the proxy to spool to disk more frequently if " +
      " you have points arriving at the proxy in short bursts")
  private int pushMemoryBufferLimit = 16 * pushFlushMaxPoints;

  @Parameter(names = {"--pushBlockedSamples"}, description = "Max number of blocked samples to print to log. Defaults" +
      " to 5.")
  private Integer pushBlockedSamples = 5;

  @Parameter(names = {"--blockedPointsLoggerName"}, description = "Logger Name for blocked " +
      "points. " + "Default: RawBlockedPoints")
  private String blockedPointsLoggerName = "RawBlockedPoints";

  @Parameter(names = {"--blockedHistogramsLoggerName"}, description = "Logger Name for blocked " +
      "histograms" + "Default: RawBlockedPoints")
  private String blockedHistogramsLoggerName = "RawBlockedPoints";

  @Parameter(names = {"--blockedSpansLoggerName"}, description =
      "Logger Name for blocked spans" + "Default: RawBlockedPoints")
  private String blockedSpansLoggerName = "RawBlockedPoints";

  @Parameter(names = {"--pushListenerPorts"}, description = "Comma-separated list of ports to listen on. Defaults to " +
      "2878.", order = 4)
  private String pushListenerPorts = "" + GRAPHITE_LISTENING_PORT;

  @Parameter(names = {"--pushListenerMaxReceivedLength"}, description = "Maximum line length for received points in" +
      " plaintext format on Wavefront/OpenTSDB/Graphite ports. Default: 32768 (32KB)")
  private Integer pushListenerMaxReceivedLength = 32768;

  @Parameter(names = {"--pushListenerHttpBufferSize"}, description = "Maximum allowed request size (in bytes) for" +
      " incoming HTTP requests on Wavefront/OpenTSDB/Graphite ports (Default: 16MB)")
  private Integer pushListenerHttpBufferSize = 16 * 1024 * 1024;

  @Parameter(names = {"--traceListenerMaxReceivedLength"}, description = "Maximum line length for received spans and" +
      " span logs (Default: 1MB)")
  private Integer traceListenerMaxReceivedLength = 1024 * 1024;

  @Parameter(names = {"--traceListenerHttpBufferSize"}, description = "Maximum allowed request size (in bytes) for" +
      " incoming HTTP requests on tracing ports (Default: 16MB)")
  private Integer traceListenerHttpBufferSize = 16 * 1024 * 1024;

  @Parameter(names = {"--listenerIdleConnectionTimeout"}, description = "Close idle inbound connections after " +
      " specified time in seconds. Default: 300")
  private int listenerIdleConnectionTimeout = 300;

  @Parameter(names = {"--memGuardFlushThreshold"}, description = "If heap usage exceeds this threshold (in percent), " +
      "flush pending points to disk as an additional OoM protection measure. Set to 0 to disable. Default: 99")
  private int memGuardFlushThreshold = 99;

  @Parameter(names = {"--histogramStateDirectory"},
      description = "Directory for persistent proxy state, must be writable.")
  private String histogramStateDirectory = "/var/spool/wavefront-proxy";

  @Parameter(names = {"--histogramAccumulatorResolveInterval"},
      description = "Interval to write-back accumulation changes from memory cache to disk in " +
          "millis (only applicable when memory cache is enabled")
  private Long histogramAccumulatorResolveInterval = 5000L;

  @Parameter(names = {"--histogramAccumulatorFlushInterval"},
      description = "Interval to check for histograms to send to Wavefront in millis. " +
          "(Default: 10000)")
  private Long histogramAccumulatorFlushInterval = 10000L;

  @Parameter(names = {"--histogramAccumulatorFlushMaxBatchSize"},
      description = "Max number of histograms to send to Wavefront in one flush " +
          "(Default: no limit)")
  private Integer histogramAccumulatorFlushMaxBatchSize = -1;

  @Parameter(names = {"--histogramMaxReceivedLength"},
      description = "Maximum line length for received histogram data (Default: 65536)")
  private Integer histogramMaxReceivedLength = 64 * 1024;

  @Parameter(names = {"--histogramHttpBufferSize"},
      description = "Maximum allowed request size (in bytes) for incoming HTTP requests on " +
          "histogram ports (Default: 16MB)")
  private Integer histogramHttpBufferSize = 16 * 1024 * 1024;

  @Parameter(names = {"--histogramMinuteListenerPorts"},
      description = "Comma-separated list of ports to listen on. Defaults to none.")
  private String histogramMinuteListenerPorts = "";

  @Parameter(names = {"--histogramMinuteFlushSecs"},
      description = "Number of seconds to keep a minute granularity accumulator open for " +
          "new samples.")
  private Integer histogramMinuteFlushSecs = 70;

  @Parameter(names = {"--histogramMinuteCompression"},
      description = "Controls allowable number of centroids per histogram. Must be in [20;1000]")
  private Short histogramMinuteCompression = 32;

  @Parameter(names = {"--histogramMinuteAvgKeyBytes"},
      description = "Average number of bytes in a [UTF-8] encoded histogram key. Generally " +
          "corresponds to a metric, source and tags concatenation.")
  private Integer histogramMinuteAvgKeyBytes = 150;

  @Parameter(names = {"--histogramMinuteAvgDigestBytes"},
      description = "Average number of bytes in a encoded histogram.")
  private Integer histogramMinuteAvgDigestBytes = 500;

  @Parameter(names = {"--histogramMinuteAccumulatorSize"},
      description = "Expected upper bound of concurrent accumulations, ~ #timeseries * #parallel " +
          "reporting bins")
  private Long histogramMinuteAccumulatorSize = 100000L;

  @Parameter(names = {"--histogramMinuteAccumulatorPersisted"}, arity = 1,
      description = "Whether the accumulator should persist to disk")
  private boolean histogramMinuteAccumulatorPersisted = false;

  @Parameter(names = {"--histogramMinuteMemoryCache"}, arity = 1,
      description = "Enabling memory cache reduces I/O load with fewer time series and higher " +
          "frequency data (more than 1 point per second per time series). Default: false")
  private boolean histogramMinuteMemoryCache = false;

  @Parameter(names = {"--histogramHourListenerPorts"},
      description = "Comma-separated list of ports to listen on. Defaults to none.")
  private String histogramHourListenerPorts = "";

  @Parameter(names = {"--histogramHourFlushSecs"},
      description = "Number of seconds to keep an hour granularity accumulator open for " +
          "new samples.")
  private Integer histogramHourFlushSecs = 4200;

  @Parameter(names = {"--histogramHourCompression"},
      description = "Controls allowable number of centroids per histogram. Must be in [20;1000]")
  private Short histogramHourCompression = 32;

  @Parameter(names = {"--histogramHourAvgKeyBytes"},
      description = "Average number of bytes in a [UTF-8] encoded histogram key. Generally " +
          " corresponds to a metric, source and tags concatenation.")
  private Integer histogramHourAvgKeyBytes = 150;

  @Parameter(names = {"--histogramHourAvgDigestBytes"},
      description = "Average number of bytes in a encoded histogram.")
  private Integer histogramHourAvgDigestBytes = 500;

  @Parameter(names = {"--histogramHourAccumulatorSize"},
      description = "Expected upper bound of concurrent accumulations, ~ #timeseries * #parallel " +
          "reporting bins")
  private Long histogramHourAccumulatorSize = 100000L;

  @Parameter(names = {"--histogramHourAccumulatorPersisted"}, arity = 1,
      description = "Whether the accumulator should persist to disk")
  private boolean histogramHourAccumulatorPersisted = false;

  @Parameter(names = {"--histogramHourMemoryCache"}, arity = 1,
      description = "Enabling memory cache reduces I/O load with fewer time series and higher " +
          "frequency data (more than 1 point per second per time series). Default: false")
  private boolean histogramHourMemoryCache = false;

  @Parameter(names = {"--histogramDayListenerPorts"},
      description = "Comma-separated list of ports to listen on. Defaults to none.")
  private String histogramDayListenerPorts = "";

  @Parameter(names = {"--histogramDayFlushSecs"},
      description = "Number of seconds to keep a day granularity accumulator open for new samples.")
  private Integer histogramDayFlushSecs = 18000;

  @Parameter(names = {"--histogramDayCompression"},
      description = "Controls allowable number of centroids per histogram. Must be in [20;1000]")
  private Short histogramDayCompression = 32;

  @Parameter(names = {"--histogramDayAvgKeyBytes"},
      description = "Average number of bytes in a [UTF-8] encoded histogram key. Generally " +
          "corresponds to a metric, source and tags concatenation.")
  private Integer histogramDayAvgKeyBytes = 150;

  @Parameter(names = {"--histogramDayAvgHistogramDigestBytes"},
      description = "Average number of bytes in a encoded histogram.")
  private Integer histogramDayAvgDigestBytes = 500;

  @Parameter(names = {"--histogramDayAccumulatorSize"},
      description = "Expected upper bound of concurrent accumulations, ~ #timeseries * #parallel " +
          "reporting bins")
  private Long histogramDayAccumulatorSize = 100000L;

  @Parameter(names = {"--histogramDayAccumulatorPersisted"}, arity = 1,
      description = "Whether the accumulator should persist to disk")
  private boolean histogramDayAccumulatorPersisted = false;

  @Parameter(names = {"--histogramDayMemoryCache"}, arity = 1,
      description = "Enabling memory cache reduces I/O load with fewer time series and higher " +
          "frequency data (more than 1 point per second per time series). Default: false")
  private boolean histogramDayMemoryCache = false;

  @Parameter(names = {"--histogramDistListenerPorts"},
      description = "Comma-separated list of ports to listen on. Defaults to none.")
  private String histogramDistListenerPorts = "";

  @Parameter(names = {"--histogramDistFlushSecs"},
      description = "Number of seconds to keep a new distribution bin open for new samples.")
  private Integer histogramDistFlushSecs = 70;

  @Parameter(names = {"--histogramDistCompression"},
      description = "Controls allowable number of centroids per histogram. Must be in [20;1000]")
  private Short histogramDistCompression = 32;

  @Parameter(names = {"--histogramDistAvgKeyBytes"},
      description = "Average number of bytes in a [UTF-8] encoded histogram key. Generally " +
          "corresponds to a metric, source and tags concatenation.")
  private Integer histogramDistAvgKeyBytes = 150;

  @Parameter(names = {"--histogramDistAvgDigestBytes"},
      description = "Average number of bytes in a encoded histogram.")
  private Integer histogramDistAvgDigestBytes = 500;

  @Parameter(names = {"--histogramDistAccumulatorSize"},
      description = "Expected upper bound of concurrent accumulations, ~ #timeseries * #parallel " +
          "reporting bins")
  private Long histogramDistAccumulatorSize = 100000L;

  @Parameter(names = {"--histogramDistAccumulatorPersisted"}, arity = 1,
      description = "Whether the accumulator should persist to disk")
  private boolean histogramDistAccumulatorPersisted = false;

  @Parameter(names = {"--histogramDistMemoryCache"}, arity = 1,
      description = "Enabling memory cache reduces I/O load with fewer time series and higher " +
          "frequency data (more than 1 point per second per time series). Default: false")
  private boolean histogramDistMemoryCache = false;

  @Parameter(names = {"--graphitePorts"}, description = "Comma-separated list of ports to listen on for graphite " +
      "data. Defaults to empty list.")
  private String graphitePorts = "";

  @Parameter(names = {"--graphiteFormat"}, description = "Comma-separated list of metric segments to extract and " +
      "reassemble as the hostname (1-based).")
  private String graphiteFormat = "";

  @Parameter(names = {"--graphiteDelimiters"}, description = "Concatenated delimiters that should be replaced in the " +
      "extracted hostname with dots. Defaults to underscores (_).")
  private String graphiteDelimiters = "_";

  @Parameter(names = {"--graphiteFieldsToRemove"}, description = "Comma-separated list of metric segments to remove (1-based)")
  private String graphiteFieldsToRemove;

  @Parameter(names = {"--jsonListenerPorts", "--httpJsonPorts"}, description = "Comma-separated list of ports to " +
      "listen on for json metrics data. Binds, by default, to none.")
  private String jsonListenerPorts = "";

  @Parameter(names = {"--dataDogJsonPorts"}, description = "Comma-separated list of ports to listen on for JSON " +
      "metrics data in DataDog format. Binds, by default, to none.")
  private String dataDogJsonPorts = "";

  @Parameter(names = {"--dataDogRequestRelayTarget"}, description = "HTTP/HTTPS target for relaying all incoming " +
      "requests on dataDogJsonPorts to. Defaults to none (do not relay incoming requests)")
  private String dataDogRequestRelayTarget = null;

  @Parameter(names = {"--dataDogProcessSystemMetrics"}, description = "If true, handle system metrics as reported by " +
      "DataDog collection agent. Defaults to false.", arity = 1)
  private boolean dataDogProcessSystemMetrics = false;

  @Parameter(names = {"--dataDogProcessServiceChecks"}, description = "If true, convert service checks to metrics. " +
      "Defaults to true.", arity = 1)
  private boolean dataDogProcessServiceChecks = true;

  @Parameter(names = {"--writeHttpJsonListenerPorts", "--writeHttpJsonPorts"}, description = "Comma-separated list " +
      "of ports to listen on for json metrics from collectd write_http json format data. Binds, by default, to none.")
  private String writeHttpJsonListenerPorts = "";

  // logs ingestion
  @Parameter(names = {"--filebeatPort"}, description = "Port on which to listen for filebeat data.")
  private Integer filebeatPort = 0;

  @Parameter(names = {"--rawLogsPort"}, description = "Port on which to listen for raw logs data.")
  private Integer rawLogsPort = 0;

  @Parameter(names = {"--rawLogsMaxReceivedLength"}, description = "Maximum line length for received raw logs (Default: 4096)")
  private Integer rawLogsMaxReceivedLength = 4096;

  @Parameter(names = {"--rawLogsHttpBufferSize"}, description = "Maximum allowed request size (in bytes) for" +
      " incoming HTTP requests with raw logs (Default: 16MB)")
  private Integer rawLogsHttpBufferSize = 16 * 1024 * 1024;

  @Parameter(names = {"--logsIngestionConfigFile"}, description = "Location of logs ingestions config yaml file.")
  private String logsIngestionConfigFile = "/etc/wavefront/wavefront-proxy/logsingestion.yaml";

  @Parameter(names = {"--hostname"}, description = "Hostname for the proxy. Defaults to FQDN of machine.")
  private String hostname = getLocalHostName();

  @Parameter(names = {"--idFile"}, description = "File to read proxy id from. Defaults to ~/.dshell/id." +
      "This property is ignored if ephemeral=true.")
  private String idFile = null;

  @Parameter(names = {"--graphiteWhitelistRegex"}, description = "(DEPRECATED for whitelistRegex)", hidden = true)
  private String graphiteWhitelistRegex;

  @Parameter(names = {"--graphiteBlacklistRegex"}, description = "(DEPRECATED for blacklistRegex)", hidden = true)
  private String graphiteBlacklistRegex;

  @Parameter(names = {"--whitelistRegex"}, description = "Regex pattern (java.util.regex) that graphite input lines must match to be accepted")
  private String whitelistRegex;

  @Parameter(names = {"--blacklistRegex"}, description = "Regex pattern (java.util.regex) that graphite input lines must NOT match to be accepted")
  private String blacklistRegex;

  @Parameter(names = {"--opentsdbPorts"}, description = "Comma-separated list of ports to listen on for opentsdb data. " +
      "Binds, by default, to none.")
  private String opentsdbPorts = "";

  @Parameter(names = {"--opentsdbWhitelistRegex"}, description = "Regex pattern (java.util.regex) that opentsdb input lines must match to be accepted")
  private String opentsdbWhitelistRegex;

  @Parameter(names = {"--opentsdbBlacklistRegex"}, description = "Regex pattern (java.util.regex) that opentsdb input lines must NOT match to be accepted")
  private String opentsdbBlacklistRegex;

  @Parameter(names = {"--picklePorts"}, description = "Comma-separated list of ports to listen on for pickle protocol " +
      "data. Defaults to none.")
  private String picklePorts;

  @Parameter(names = {"--traceListenerPorts"}, description = "Comma-separated list of ports to listen on for trace " +
      "data. Defaults to none.")
  private String traceListenerPorts;

  @Parameter(names = {"--traceJaegerListenerPorts"}, description = "Comma-separated list of ports on which to listen " +
      "on for jaeger thrift formatted data over TChannel protocol. Defaults to none.")
  private String traceJaegerListenerPorts;

  @Parameter(names = {"--traceJaegerApplicationName"}, description = "Application name for Jaeger. Defaults to Jaeger.")
  private String traceJaegerApplicationName;

  @Parameter(names = {"--traceZipkinListenerPorts"}, description = "Comma-separated list of ports on which to listen " +
      "on for zipkin trace data over HTTP. Defaults to none.")
  private String traceZipkinListenerPorts;

  @Parameter(names = {"--traceZipkinApplicationName"}, description = "Application name for Zipkin. Defaults to Zipkin.")
  private String traceZipkinApplicationName;

  @Parameter(names = {"--traceSamplingRate"}, description = "Value between 0.0 and 1.0. " +
      "Defaults to 1.0 (allow all spans).")
  private double traceSamplingRate = 1.0d;

  @Parameter(names = {"--traceSamplingDuration"}, description = "Sample spans by duration in " +
      "milliseconds. " + "Defaults to 0 (ignore duration based sampling).")
  private Integer traceSamplingDuration = 0;

  @Parameter(names = {"--traceDerivedCustomTagKeys"}, description = "Comma-separated " +
      "list of custom tag keys for trace derived RED metrics.")
  private String traceDerivedCustomTagKeys;

  @Parameter(names = {"--traceAlwaysSampleErrors"}, description = "Always sample spans with error tag (set to true) " +
      "ignoring other sampling configuration. Defaults to true.", arity = 1)
  private boolean traceAlwaysSampleErrors = true;

  @Parameter(names = {"--pushRelayListenerPorts"}, description = "Comma-separated list of ports on which to listen " +
      "on for proxy chaining data. For internal use. Defaults to none.")
  private String pushRelayListenerPorts;

  @Parameter(names = {"--pushRelayHistogramAggregator"}, description = "If true, aggregate " +
      "histogram distributions received on the relay port. Default: false", arity = 1)
  private boolean pushRelayHistogramAggregator = false;

  @Parameter(names = {"--pushRelayHistogramAggregatorAccumulatorSize"},
      description = "Expected upper bound of concurrent accumulations, ~ #timeseries * #parallel " +
          "reporting bins")
  private Long pushRelayHistogramAggregatorAccumulatorSize = 100000L;

  @Parameter(names = {"--pushRelayHistogramAggregatorFlushSecs"},
      description = "Number of seconds to keep a day granularity accumulator open for new samples.")
  private Integer pushRelayHistogramAggregatorFlushSecs = 70;

  @Parameter(names = {"--pushRelayHistogramAggregatorCompression"},
      description = "Controls allowable number of centroids per histogram. Must be in [20;1000] " +
          "range. Default: 32")
  private Short pushRelayHistogramAggregatorCompression = 32;

  @Parameter(names = {"--splitPushWhenRateLimited"}, description = "Whether to split the push " +
      "batch size when the push is rejected by Wavefront due to rate limit.  Default false.",
      arity = 1)
  private boolean splitPushWhenRateLimited = DEFAULT_SPLIT_PUSH_WHEN_RATE_LIMITED;

  @Parameter(names = {"--retryBackoffBaseSeconds"}, description = "For exponential backoff " +
      "when retry threads are throttled, the base (a in a^b) in seconds.  Default 2.0")
  private double retryBackoffBaseSeconds = DEFAULT_RETRY_BACKOFF_BASE_SECONDS;

  @Parameter(names = {"--customSourceTags"}, description = "Comma separated list of point tag " +
      "keys that should be treated as the source in Wavefront in the absence of a tag named " +
      "`source` or `host`. Default: fqdn")
  private String customSourceTags = "fqdn";

  @Parameter(names = {"--agentMetricsPointTags"}, description = "Additional point tags and their " +
      " respective values to be included into internal agent's metrics " +
      "(comma-separated list, ex: dc=west,env=prod). Default: none")
  private String agentMetricsPointTags = null;

  @Parameter(names = {"--ephemeral"}, arity = 1, description = "If true, this proxy is removed " +
      "from Wavefront after 24 hours of inactivity. Default: true")
  private boolean ephemeral = true;

  @Parameter(names = {"--disableRdnsLookup"}, arity = 1, description = "When receiving" +
      " Wavefront-formatted data without source/host specified, use remote IP address as source " +
      "instead of trying to resolve the DNS name. Default false.")
  private boolean disableRdnsLookup = false;

  @Parameter(names = {"--gzipCompression"}, arity = 1, description = "If true, enables gzip " +
      "compression for traffic sent to Wavefront (Default: true)")
  private boolean gzipCompression = true;

  @Parameter(names = {"--soLingerTime"}, description = "If provided, enables SO_LINGER with the specified linger time in seconds (default: SO_LINGER disabled)")
  private Integer soLingerTime = -1;

  @Parameter(names = {"--proxyHost"}, description = "Proxy host for routing traffic through a http proxy")
  private String proxyHost = null;

  @Parameter(names = {"--proxyPort"}, description = "Proxy port for routing traffic through a http proxy")
  private Integer proxyPort = 0;

  @Parameter(names = {"--proxyUser"}, description = "If proxy authentication is necessary, this is the username that will be passed along")
  private String proxyUser = null;

  @Parameter(names = {"--proxyPassword"}, description = "If proxy authentication is necessary, this is the password that will be passed along")
  private String proxyPassword = null;

  @Parameter(names = {"--httpUserAgent"}, description = "Override User-Agent in request headers")
  private String httpUserAgent = null;

  @Parameter(names = {"--httpConnectTimeout"}, description = "Connect timeout in milliseconds (default: 5000)")
  private Integer httpConnectTimeout = 5000;

  @Parameter(names = {"--httpRequestTimeout"}, description = "Request timeout in milliseconds (default: 10000)")
  private Integer httpRequestTimeout = 10000;

  @Parameter(names = {"--httpMaxConnTotal"}, description = "Max connections to keep open (default: 200)")
  private Integer httpMaxConnTotal = 200;

  @Parameter(names = {"--httpMaxConnPerRoute"}, description = "Max connections per route to keep open (default: 100)")
  private Integer httpMaxConnPerRoute = 100;

  @Parameter(names = {"--httpAutoRetries"}, description = "Number of times to retry http requests before queueing, set to 0 to disable (default: 3)")
  private Integer httpAutoRetries = 3;

  @Parameter(names = {"--preprocessorConfigFile"}, description = "Optional YAML file with additional configuration options for filtering and pre-processing points")
  private String preprocessorConfigFile = null;

  @Parameter(names = {"--dataBackfillCutoffHours"}, description = "The cut-off point for what is considered a valid timestamp for back-dated points. Default is 8760 (1 year)")
  private int dataBackfillCutoffHours = 8760;

  @Parameter(names = {"--dataPrefillCutoffHours"}, description = "The cut-off point for what is considered a valid timestamp for pre-dated points. Default is 24 (1 day)")
  private int dataPrefillCutoffHours = 24;

  @Parameter(names = {"--authMethod"}, converter = TokenValidationMethod.TokenValidationMethodConverter.class,
      description = "Authenticate all incoming HTTP requests and disables TCP streams when set to a value " +
          "other than NONE. Allowed values are: NONE, STATIC_TOKEN, HTTP_GET, OAUTH2. Default: NONE")
  private TokenValidationMethod authMethod = TokenValidationMethod.NONE;

  @Parameter(names = {"--authTokenIntrospectionServiceUrl"}, description = "URL for the token introspection endpoint " +
      "used to validate tokens for incoming HTTP requests. Required for authMethod = OAUTH2 (endpoint must be " +
      "RFC7662-compliant) and authMethod = HTTP_GET (use {{token}} placeholder in the URL to pass token to the " +
      "service, endpoint must return any 2xx status for valid tokens, any other response code is a fail)")
  private String authTokenIntrospectionServiceUrl = null;

  @Parameter(names = {"--authTokenIntrospectionAuthorizationHeader"}, description = "Optional credentials for use " +
      "with the token introspection endpoint.")
  private String authTokenIntrospectionAuthorizationHeader = null;

  @Parameter(names = {"--authResponseRefreshInterval"}, description = "Cache TTL (in seconds) for token validation " +
      "results (re-authenticate when expired). Default: 600 seconds")
  private int authResponseRefreshInterval = 600;

  @Parameter(names = {"--authResponseMaxTtl"}, description = "Maximum allowed cache TTL (in seconds) for token " +
      "validation results when token introspection service is unavailable. Default: 86400 seconds (1 day)")
  private int authResponseMaxTtl = 86400;

  @Parameter(names = {"--authStaticToken"}, description = "Static token that is considered valid " +
      "for all incoming HTTP requests. Required when authMethod = STATIC_TOKEN.")
  private String authStaticToken = null;

  @Parameter(names = {"--adminApiListenerPort"}, description = "Enables admin port to control " +
      "healthcheck status per port. Default: none")
  private Integer adminApiListenerPort = 0;

  @Parameter(names = {"--adminApiRemoteIpWhitelistRegex"}, description = "Remote IPs must match " +
      "this regex to access admin API")
  private String adminApiRemoteIpWhitelistRegex = null;

  @Parameter(names = {"--httpHealthCheckPorts"}, description = "Comma-delimited list of ports " +
      "to function as standalone healthchecks. May be used independently of " +
      "--httpHealthCheckAllPorts parameter. Default: none")
  private String httpHealthCheckPorts = null;

  @Parameter(names = {"--httpHealthCheckAllPorts"}, description = "When true, all listeners that " +
      "support HTTP protocol also respond to healthcheck requests. May be used independently of " +
      "--httpHealthCheckPorts parameter. Default: false", arity = 1)
  private boolean httpHealthCheckAllPorts = false;

  @Parameter(names = {"--httpHealthCheckPath"}, description = "Healthcheck's path, for example, " +
      "'/health'. Default: '/'")
  private String httpHealthCheckPath = "/";

  @Parameter(names = {"--httpHealthCheckResponseContentType"}, description = "Optional " +
      "Content-Type to use in healthcheck response, for example, 'application/json'. Default: none")
  private String httpHealthCheckResponseContentType = null;

  @Parameter(names = {"--httpHealthCheckPassStatusCode"}, description = "HTTP status code for " +
      "'pass' health checks. Default: 200")
  private int httpHealthCheckPassStatusCode = 200;

  @Parameter(names = {"--httpHealthCheckPassResponseBody"}, description = "Optional response " +
      "body to return with 'pass' health checks. Default: none")
  private String httpHealthCheckPassResponseBody = null;

  @Parameter(names = {"--httpHealthCheckFailStatusCode"}, description = "HTTP status code for " +
      "'fail' health checks. Default: 503")
  private int httpHealthCheckFailStatusCode = 503;

  @Parameter(names = {"--httpHealthCheckFailResponseBody"}, description = "Optional response " +
      "body to return with 'fail' health checks. Default: none")
  private String httpHealthCheckFailResponseBody = null;

  @Parameter(names = {"--deltaCountersAggregationIntervalSeconds"},
      description = "Delay time for delta counter reporter. Defaults to 30 seconds.")
  private long deltaCountersAggregationIntervalSeconds = 30;

  @Parameter(names = {"--deltaCountersAggregationListenerPorts"},
      description = "Comma-separated list of ports to listen on Wavefront-formatted delta " +
          "counters. Helps reduce outbound point rate by pre-aggregating delta counters at proxy." +
          " Defaults: none")
  private String deltaCountersAggregationListenerPorts = "";

  @Parameter()
  private List<String> unparsed_params;

  public boolean isHelp() {
    return help;
  }

  public boolean isVersion() {
    return version;
  }

  public String getPushConfigFile() {
    return pushConfigFile;
  }

  public String getConfigFile() {
    return configFile;
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

  public String getPushValidationLevel() {
    return pushValidationLevel;
  }

  public String getServer() {
    return server;
  }

  public String getBufferFile() {
    return bufferFile;
  }

  public Integer getRetryThreads() {
    return retryThreads;
  }

  public Integer getFlushThreads() {
    return flushThreads;
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

  public Integer getPushRateLimit() {
    return pushRateLimit;
  }

  public Integer getPushRateLimitHistograms() {
    return pushRateLimitHistograms;
  }

  public Double getPushRateLimitSourceTags() {
    return pushRateLimitSourceTags;
  }

  public Integer getPushRateLimitSpans() {
    return pushRateLimitSpans;
  }

  public Integer getPushRateLimitSpanLogs() {
    return pushRateLimitSpanLogs;
  }

  public Double getPushRateLimitEvents() {
    return pushRateLimitEvents;
  }

  public Integer getPushRateLimitMaxBurstSeconds() {
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

  public String getGraphiteWhitelistRegex() {
    return graphiteWhitelistRegex;
  }

  public String getGraphiteBlacklistRegex() {
    return graphiteBlacklistRegex;
  }

  public String getWhitelistRegex() {
    return whitelistRegex;
  }

  public String getBlacklistRegex() {
    return blacklistRegex;
  }

  public String getOpentsdbPorts() {
    return opentsdbPorts;
  }

  public String getOpentsdbWhitelistRegex() {
    return opentsdbWhitelistRegex;
  }

  public String getOpentsdbBlacklistRegex() {
    return opentsdbBlacklistRegex;
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

  public String getTraceJaegerApplicationName() {
    return traceJaegerApplicationName;
  }

  public String getTraceZipkinListenerPorts() {
    return traceZipkinListenerPorts;
  }

  public String getTraceZipkinApplicationName() {
    return traceZipkinApplicationName;
  }

  public double getTraceSamplingRate() {
    return traceSamplingRate;
  }

  public Integer getTraceSamplingDuration() {
    return traceSamplingDuration;
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

  public String getAdminApiRemoteIpWhitelistRegex() {
    return adminApiRemoteIpWhitelistRegex;
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

  public Set<String> getTraceDerivedCustomTagKeys() {
    return new HashSet<>(Splitter.on(",").trimResults().omitEmptyStrings().
        splitToList(ObjectUtils.firstNonNull(traceDerivedCustomTagKeys, "")));
  }

  @Override
  public void verifyAndInit() throws ConfigurationException {
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
      pushValidationLevel = config.getString("pushValidationLevel", pushValidationLevel);
      // don't track token in proxy config metrics
      token = ObjectUtils.firstNonNull(config.getRawProperty("token", token), "undefined").trim();
      server = config.getRawProperty("server", server).trim();
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

      retryThreads = config.getInteger("retryThreads", retryThreads);
      flushThreads = config.getInteger("flushThreads", flushThreads);
      jsonListenerPorts = config.getString("jsonListenerPorts", jsonListenerPorts);
      writeHttpJsonListenerPorts = config.getString("writeHttpJsonListenerPorts",
          writeHttpJsonListenerPorts);
      dataDogJsonPorts = config.getString("dataDogJsonPorts", dataDogJsonPorts);
      dataDogRequestRelayTarget = config.getString("dataDogRequestRelayTarget",
          dataDogRequestRelayTarget);
      dataDogProcessSystemMetrics = config.getBoolean("dataDogProcessSystemMetrics",
          dataDogProcessSystemMetrics);
      dataDogProcessServiceChecks = config.getBoolean("dataDogProcessServiceChecks",
          dataDogProcessServiceChecks);
      graphitePorts = config.getString("graphitePorts", graphitePorts);
      graphiteFormat = config.getString("graphiteFormat", graphiteFormat);
      graphiteFieldsToRemove = config.getString("graphiteFieldsToRemove", graphiteFieldsToRemove);
      graphiteDelimiters = config.getString("graphiteDelimiters", graphiteDelimiters);
      graphiteWhitelistRegex = config.getString("graphiteWhitelistRegex", graphiteWhitelistRegex);
      graphiteBlacklistRegex = config.getString("graphiteBlacklistRegex", graphiteBlacklistRegex);
      whitelistRegex = config.getString("whitelistRegex", whitelistRegex);
      blacklistRegex = config.getString("blacklistRegex", blacklistRegex);
      opentsdbPorts = config.getString("opentsdbPorts", opentsdbPorts);
      opentsdbWhitelistRegex = config.getString("opentsdbWhitelistRegex", opentsdbWhitelistRegex);
      opentsdbBlacklistRegex = config.getString("opentsdbBlacklistRegex", opentsdbBlacklistRegex);
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
      traceJaegerApplicationName = config.getString("traceJaegerApplicationName",
          traceJaegerApplicationName);
      traceZipkinListenerPorts = config.getString("traceZipkinListenerPorts",
          traceZipkinListenerPorts);
      traceZipkinApplicationName = config.getString("traceZipkinApplicationName",
          traceZipkinApplicationName);
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
      pushRelayHistogramAggregatorFlushSecs =
          config.getInteger("pushRelayHistogramAggregatorFlushSecs",
              pushRelayHistogramAggregatorFlushSecs);
      pushRelayHistogramAggregatorCompression =
          config.getNumber("pushRelayHistogramAggregatorCompression",
              pushRelayHistogramAggregatorCompression).shortValue();
      bufferFile = config.getString("buffer", bufferFile);
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

      authMethod = TokenValidationMethod.fromString(config.getString("authMethod",
          authMethod.toString()));
      authTokenIntrospectionServiceUrl = config.getString("authTokenIntrospectionServiceUrl",
          authTokenIntrospectionServiceUrl);
      authTokenIntrospectionAuthorizationHeader =
          config.getString("authTokenIntrospectionAuthorizationHeader",
              authTokenIntrospectionAuthorizationHeader);
      authResponseRefreshInterval = config.getInteger("authResponseRefreshInterval",
          authResponseRefreshInterval);
      authResponseMaxTtl = config.getInteger("authResponseMaxTtl", authResponseMaxTtl);
      authStaticToken = config.getString("authStaticToken", authStaticToken);

      adminApiListenerPort = config.getInteger("adminApiListenerPort", adminApiListenerPort);
      adminApiRemoteIpWhitelistRegex = config.getString("adminApiRemoteIpWhitelistRegex",
          adminApiRemoteIpWhitelistRegex);
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

      // clamp values for pushFlushMaxPoints/etc between min split size
      // (or 1 in case of source tags and events) and default batch size.
      // also make sure it is never higher than the configured rate limit.
      pushFlushMaxPoints = Math.min(Math.min(Math.max(config.getInteger("pushFlushMaxPoints",
          pushFlushMaxPoints), DEFAULT_MIN_SPLIT_BATCH_SIZE), DEFAULT_BATCH_SIZE), pushRateLimit);
      pushFlushMaxHistograms = Math.min(Math.min(Math.max(config.getInteger(
          "pushFlushMaxHistograms", pushFlushMaxHistograms), DEFAULT_MIN_SPLIT_BATCH_SIZE),
          DEFAULT_BATCH_SIZE_HISTOGRAMS), pushRateLimitHistograms);
      pushFlushMaxSourceTags = Math.min(Math.min(Math.max(config.getInteger(
          "pushFlushMaxSourceTags", pushFlushMaxSourceTags), 1),
          DEFAULT_BATCH_SIZE_SOURCE_TAGS), pushRateLimitSourceTags.intValue());
      pushFlushMaxSpans = Math.min(Math.min(Math.max(config.getInteger("pushFlushMaxSpans",
          pushFlushMaxSpans), DEFAULT_MIN_SPLIT_BATCH_SIZE), DEFAULT_BATCH_SIZE_SPANS),
          pushRateLimitSpans);
      pushFlushMaxSpanLogs = Math.min(Math.min(Math.max(config.getInteger("pushFlushMaxSpanLogs",
          pushFlushMaxSpanLogs), DEFAULT_MIN_SPLIT_BATCH_SIZE), DEFAULT_BATCH_SIZE_SPAN_LOGS),
          pushRateLimitSpanLogs);
      pushFlushMaxEvents = Math.min(Math.min(Math.max(config.getInteger("pushFlushMaxEvents",
          pushFlushMaxEvents), 1), DEFAULT_BATCH_SIZE_EVENTS), pushRateLimitEvents.intValue());

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
    // Compatibility with deprecated fields
    if (whitelistRegex == null && graphiteWhitelistRegex != null) {
      whitelistRegex = graphiteWhitelistRegex;
    }
    if (blacklistRegex == null && graphiteBlacklistRegex != null) {
      blacklistRegex = graphiteBlacklistRegex;
    }
    if (httpUserAgent == null) {
      httpUserAgent = "Wavefront-Proxy/" + getBuildVersion();
    }
    if (pushConfigFile != null) {
      logger.info("Loaded configuration file " + pushConfigFile);
    }
  }
}

package com.wavefront.agent.handlers;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.util.concurrent.AtomicDouble;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Clock;
import com.wavefront.common.HostMetricTagsPair;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.data.Validation;
import com.wavefront.ingester.ReportPointSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.DeltaCounter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import org.apache.commons.lang.math.NumberUtils;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.yammer.metrics.core.Counter;
import javax.annotation.Nullable;
import wavefront.report.ReportPoint;
import static com.wavefront.data.Validation.validatePoint;
import static com.wavefront.sdk.common.Utils.metricToLineData;
import com.yammer.metrics.core.Gauge;

/**
 * Handler that processes incoming DeltaCounter objects, validates them and hands them over to one of
 * the {@link SenderTask} threads.
 *
 * @author djia@vwware.com
 */
public class DeltaCounterHandlerImpl extends AbstractReportableEntityHandler<ReportPoint> {

    private static final Logger logger = Logger.getLogger(
        AbstractReportableEntityHandler.class.getCanonicalName());
    private static final Logger validPointsLogger = Logger.getLogger("RawValidPoints");
    private static final Random RANDOM = new Random();

    final Histogram receivedPointLag;
    private final Counter receivedCounter;
    private final Counter reportedCounter;
    private final Counter rejectedCounter;
    private long estimatedSize;


    boolean logData = false;
    private final double logSampleRate;
    private volatile long logStateUpdatedMillis = 0L;
    private final Cache<HostMetricTagsPair, AtomicDouble> metricCache;
    private final ScheduledExecutorService deltaCounterReporter =
        Executors.newSingleThreadScheduledExecutor();

    /**
     * Value of system property wavefront.proxy.logpoints (for backwards compatibility)
     */
    private final boolean logPointsFlag;

    /**
     * Creates a new instance that handles either histograms or points.
     *
     * @param handle               handle/port number
     * @param blockedItemsPerBatch controls sample rate of how many blocked points are written into
     *                             the main log file.
     * @param senderTasks          sender tasks
     * @param validationConfig     Supplier for the validation configuration. if false).
     * @param setupMetrics         Whether we should report counter metrics.
     */
    DeltaCounterHandlerImpl(final String handle,
                            final int blockedItemsPerBatch,
                            final Collection<SenderTask> senderTasks,
                            @Nullable final Supplier<ValidationConfiguration> validationConfig,
                            final ReportableEntityType reportableEntityType,
                            final boolean setupMetrics,
                            long reportIntervalSeconds) {
        super(reportableEntityType, handle, blockedItemsPerBatch, new ReportPointSerializer(),
            senderTasks, validationConfig, "pps", setupMetrics);

        this.metricCache = Caffeine.newBuilder()
                .expireAfterWrite(15, TimeUnit.MINUTES).
                        removalListener((RemovalListener<HostMetricTagsPair, AtomicDouble>)(metric, value, reason) ->
                                this.reportDeltaCounter(metric, value)).build();


        String logPointsProperty = System.getProperty("wavefront.proxy.logpoints");
        this.logPointsFlag = logPointsProperty != null && logPointsProperty.equalsIgnoreCase("true");
        String logPointsSampleRateProperty =
            System.getProperty("wavefront.proxy.logpoints.sample-rate");
        this.logSampleRate = NumberUtils.isNumber(logPointsSampleRateProperty) ?
            Double.parseDouble(logPointsSampleRateProperty) : 1.0d;

        MetricsRegistry registry = setupMetrics ? Metrics.defaultRegistry() : new MetricsRegistry();
        this.receivedPointLag = registry.newHistogram(new MetricName("points." + handle + ".received",
            "", "lag"), false);

        deltaCounterReporter.scheduleWithFixedDelay(
            this::reportCache, reportIntervalSeconds, reportIntervalSeconds, TimeUnit.SECONDS);

        String metricPrefix = entityType.toString() + "." + handle;
        this.receivedCounter = registry.newCounter(new MetricName(metricPrefix, "", "received"));
        this.reportedCounter = registry.newCounter(new MetricName(metricPrefix, "", "sent"));
        this.rejectedCounter = registry.newCounter(new MetricName(metricPrefix, "", "rejected"));
        estimatedSize = this.metricCache.estimatedSize();
        Metrics.newGauge(new MetricName("cache", "", "estimated-size"),
                new Gauge<Long>() {
                    @Override
                    public Long value() {
                        return estimatedSize;
                    }
                });
    }

    private void reportCache() {
        this.metricCache.asMap().forEach(this::reportDeltaCounter);
    }

    private void reportDeltaCounter(HostMetricTagsPair hostMetricTagsPair, AtomicDouble value) {
        this.reportedCounter.inc();
        if (value == null || hostMetricTagsPair == null) {return;}
        double reportedValue = value.getAndSet(0);
        if(reportedValue == 0) return;
        String strPoint = metricToLineData(hostMetricTagsPair.metric, reportedValue,
            System.currentTimeMillis(), hostMetricTagsPair.getHost(),
            hostMetricTagsPair.getTags(), "wavefront-proxy");
        getTask().add(strPoint);
    }

    @Override
    @SuppressWarnings("unchecked")
    void reportInternal(ReportPoint point) {
        if (validationConfig.get() == null) {
            validatePoint(point, handle, Validation.Level.NUMERIC_ONLY);
        } else {
            validatePoint(point, validationConfig.get());
        }
        if (DeltaCounter.isDelta(point.getMetric())) {
            String strPoint = serializer.apply(point);

            refreshValidPointsLoggerState();

            if ((logData || logPointsFlag) &&
                (logSampleRate >= 1.0d || (logSampleRate > 0.0d && RANDOM.nextDouble() < logSampleRate))) {
                // we log valid points only if system property wavefront.proxy.logpoints is true
                // or RawValidPoints log level is set to "ALL". this is done to prevent introducing
                // overhead and accidentally logging points to the main log.
                // Additionally, honor sample rate limit, if set.
                validPointsLogger.info(strPoint);
            }

            getReceivedCounter().inc();

            double pvalue = Double.parseDouble(String.valueOf(point.getValue()));
            receivedPointLag.update(Clock.now() - point.getTimestamp());
            HostMetricTagsPair hostMetricTagsPair = new HostMetricTagsPair(point.getHost(),
                point.getMetric(), point.getAnnotations());
            metricCache.get(hostMetricTagsPair,
                key -> new AtomicDouble(0)).getAndAdd(pvalue);
            this.receivedCounter.inc();
        } else {
            reject(point, "Port is not configured to accept non-delta counter data!");
            this.rejectedCounter.inc();
        }

    }

    void refreshValidPointsLoggerState() {
        if (logStateUpdatedMillis + TimeUnit.SECONDS.toMillis(1) < System.currentTimeMillis()) {
            // refresh validPointsLogger level once a second
            if (logData != validPointsLogger.isLoggable(Level.FINEST)) {
                logData = !logData;
                logger.info("Valid " + entityType.toString() + " logging is now " + (logData ?
                    "enabled with " + (logSampleRate * 100) + "% sampling" :
                    "disabled"));
            }
            logStateUpdatedMillis = System.currentTimeMillis();
        }
    }

}

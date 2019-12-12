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

import org.apache.commons.lang.math.NumberUtils;
import java.util.Collection;
import java.util.Objects;
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
 * Handler that processes incoming DeltaCounter objects, aggregates them and hands it over to one
 * of the {@link SenderTask} threads according to deltaCountersAggregationIntervalSeconds or
 * before cache expires.
 *
 * @author djia@vmware.com
 */
public class DeltaCounterAccumulationHandlerImpl
    extends AbstractReportableEntityHandler<ReportPoint, String> {

    private static final Logger logger = Logger.getLogger(
        DeltaCounterAccumulationHandlerImpl.class.getCanonicalName());
    private static final Logger validPointsLogger = Logger.getLogger("RawValidPoints");
    private static final Random RANDOM = new Random();
    final Histogram receivedPointLag;
    private final Counter reportedCounter;
    boolean logData = false;
    private final double logSampleRate;
    private volatile long logStateUpdatedMillis = 0L;
    private final Cache<HostMetricTagsPair, AtomicDouble> aggregatedDeltas;
    private final ScheduledExecutorService deltaCounterReporter =
        Executors.newSingleThreadScheduledExecutor();

    /**
     * Value of system property wavefront.proxy.logpoints (for backwards compatibility)
     */
    private final boolean logPointsFlag;

    /**
     * Creates a new instance that handles either histograms or points.
     *
     * @param handle               handle/port number.
     * @param blockedItemsPerBatch controls sample rate of how many blocked points are written into
     *                             the main log file.
     * @param validationConfig     Supplier for the validation configuration. if false).
     */
    public DeltaCounterAccumulationHandlerImpl(final String handle,
                                               final int blockedItemsPerBatch,
                                               final Collection<SenderTask<String>> senderTasks,
                                               @Nullable final Supplier<ValidationConfiguration> validationConfig,
                                               long deltaCountersAggregationIntervalSeconds,
                                               final Logger blockedItemLogger) {
        super(ReportableEntityType.DELTA_COUNTER, handle, blockedItemsPerBatch,
            new ReportPointSerializer(), senderTasks, validationConfig, "pps", true,
            blockedItemLogger);

        this.aggregatedDeltas = Caffeine.newBuilder().
            expireAfterAccess(5 * deltaCountersAggregationIntervalSeconds, TimeUnit.SECONDS).
            removalListener((RemovalListener<HostMetricTagsPair, AtomicDouble>)
                (metric, value, reason) -> this.reportAggregatedDeltaValue(metric, value)).build();

        String logPointsProperty = System.getProperty("wavefront.proxy.logpoints");
        this.logPointsFlag =
            logPointsProperty != null && logPointsProperty.equalsIgnoreCase("true");
        String logPointsSampleRateProperty =
            System.getProperty("wavefront.proxy.logpoints.sample-rate");
        this.logSampleRate = NumberUtils.isNumber(logPointsSampleRateProperty) ?
            Double.parseDouble(logPointsSampleRateProperty) : 1.0d;

        this.receivedPointLag = Metrics.newHistogram(new MetricName("points." + handle +
            ".received", "", "lag"), false);

        deltaCounterReporter.scheduleWithFixedDelay(this::reportCache,
            deltaCountersAggregationIntervalSeconds, deltaCountersAggregationIntervalSeconds,
            TimeUnit.SECONDS);

        String metricPrefix = entityType.toString() + "." + handle;
        this.reportedCounter = Metrics.newCounter(new MetricName(metricPrefix, "",
            "sent"));
        Metrics.newGauge(new MetricName(metricPrefix, "",
            "accumulator.size"), new Gauge<Long>() {
            @Override
            public Long value() {
                return aggregatedDeltas.estimatedSize();
            }
        });
    }

    private void reportCache() {
        this.aggregatedDeltas.asMap().forEach(this::reportAggregatedDeltaValue);
    }

    private void reportAggregatedDeltaValue(@Nullable HostMetricTagsPair hostMetricTagsPair,
                                            @Nullable AtomicDouble value) {
        this.reportedCounter.inc();
        if (value == null || hostMetricTagsPair == null) {return;}
        double reportedValue = value.getAndSet(0);
        if (reportedValue == 0) return;
        String strPoint = metricToLineData(hostMetricTagsPair.metric, reportedValue,
            Clock.now(), hostMetricTagsPair.getHost(),
            hostMetricTagsPair.getTags(), "wavefront-proxy");
        getTask().add(strPoint);
    }

    @Override
    void reportInternal(ReportPoint point) {
        if (validationConfig.get() == null) {
            validatePoint(point, handle, Validation.Level.NUMERIC_ONLY);
        } else {
            validatePoint(point, validationConfig.get());
        }
        if (DeltaCounter.isDelta(point.getMetric())) {
            refreshValidPointsLoggerState();
            if ((logData || logPointsFlag) &&
                (logSampleRate >= 1.0d || (logSampleRate > 0.0d && RANDOM.nextDouble() < logSampleRate))) {
                // we log valid points only if system property wavefront.proxy.logpoints is true
                // or RawValidPoints log level is set to "ALL". this is done to prevent introducing
                // overhead and accidentally logging points to the main log.
                // Additionally, honor sample rate limit, if set.
                String strPoint = serializer.apply(point);
                validPointsLogger.info(strPoint);
            }
            getReceivedCounter().inc();
            double deltaValue = (double) point.getValue();
            receivedPointLag.update(Clock.now() - point.getTimestamp());
            HostMetricTagsPair hostMetricTagsPair = new HostMetricTagsPair(point.getHost(),
                point.getMetric(), point.getAnnotations());
            Objects.requireNonNull(aggregatedDeltas.get(hostMetricTagsPair,
                key -> new AtomicDouble(0))).getAndAdd(deltaValue);
            getReceivedCounter().inc();
        } else {
            reject(point, "Port is not configured to accept non-delta counter data!");
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

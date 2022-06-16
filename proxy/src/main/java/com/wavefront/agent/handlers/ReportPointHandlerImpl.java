package com.wavefront.agent.handlers;

import static com.wavefront.data.Validation.validatePoint;

import com.wavefront.agent.buffer.BuffersManager;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Clock;
import com.wavefront.common.Pair;
import com.wavefront.common.Utils;
import com.wavefront.data.DeltaCounterValueException;
import com.wavefront.ingester.ReportPointSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.activemq.artemis.api.core.client.*;
import wavefront.report.Histogram;
import wavefront.report.ReportPoint;

/**
 * Handler that processes incoming ReportPoint objects, validates them and hands them over to one of
 * the {@link SenderTask} threads.
 *
 * @author vasily@wavefront.com
 */
class ReportPointHandlerImpl extends AbstractReportableEntityHandler<ReportPoint, String> {
  private static final Logger logger =
      Logger.getLogger(ReportPointHandlerImpl.class.getCanonicalName());

  final Logger validItemsLogger;
  final ValidationConfiguration validationConfig;
  final Function<Histogram, Histogram> recompressor;
  final com.yammer.metrics.core.Histogram receivedPointLag;
  final com.yammer.metrics.core.Histogram receivedTagCount;
  final Supplier<Counter> discardedCounterSupplier;

  private final Map<String, Pair<ClientSession, ClientProducer>> mqContext = new HashMap<>();
  private final Map<String, Pair<ClientSession, ClientProducer>> mqContextDisk = new HashMap<>();

  /**
   * Creates a new instance that handles either histograms or points.
   *
   * @param handlerKey handler key for the metrics pipeline.
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written into
   *     the main log file.
   * @param senderTaskMap map of tenant name and tasks actually handling data transfer to the
   *     Wavefront endpoint corresponding to the tenant name
   * @param validationConfig validation configuration.
   * @param setupMetrics Whether we should report counter metrics.
   * @param receivedRateSink Where to report received rate.
   * @param blockedItemLogger logger for blocked items (optional).
   * @param validItemsLogger sampling logger for valid items (optional).
   * @param recompressor histogram recompressor (optional)
   */
  ReportPointHandlerImpl(
      final HandlerKey handlerKey,
      final int blockedItemsPerBatch,
      @Nullable final Map<String, Collection<SenderTask>> senderTaskMap,
      @Nonnull final ValidationConfiguration validationConfig,
      final boolean setupMetrics,
      @Nullable final BiConsumer<String, Long> receivedRateSink,
      @Nullable final Logger blockedItemLogger,
      @Nullable final Logger validItemsLogger,
      @Nullable final Function<Histogram, Histogram> recompressor) {
    super(
        handlerKey,
        blockedItemsPerBatch,
        new ReportPointSerializer(),
        senderTaskMap,
        setupMetrics,
        receivedRateSink,
        blockedItemLogger);
    this.validationConfig = validationConfig;
    this.validItemsLogger = validItemsLogger;
    this.recompressor = recompressor;
    MetricsRegistry registry = setupMetrics ? Metrics.defaultRegistry() : LOCAL_REGISTRY;
    this.receivedPointLag =
        registry.newHistogram(
            new MetricName(handlerKey.getQueue() + ".received", "", "lag"), false);
    this.receivedTagCount =
        registry.newHistogram(
            new MetricName(handlerKey.getQueue() + ".received", "", "tagCount"), false);
    this.discardedCounterSupplier =
        Utils.lazySupplier(
            () -> Metrics.newCounter(new MetricName(handlerKey.toString(), "", "discarded")));
    logger.severe("ReportPointHandlerImpl created");
  }

  @Override
  void reportInternal(ReportPoint point) {
    //    logger.severe("reportInternal " + Thread.currentThread().getName());
    receivedTagCount.update(point.getAnnotations().size());
    try {
      validatePoint(point, validationConfig);
    } catch (DeltaCounterValueException e) {
      discardedCounterSupplier.get().inc();
      return;
    }
    receivedPointLag.update(Clock.now() - point.getTimestamp());
    if (point.getValue() instanceof Histogram && recompressor != null) {
      Histogram histogram = (Histogram) point.getValue();
      point.setValue(recompressor.apply(histogram));
    }
    final String strPoint = serializer.apply(point);

    getReceivedCounter().inc();
    BuffersManager.sendMsg(handlerKey, Collections.singletonList(strPoint));

    if (validItemsLogger != null) validItemsLogger.info(strPoint);
  }

  private void toDisk(String strPoint) {
    System.out.println("--> msg toDisk ");
    Pair<ClientSession, ClientProducer> mqCtx =
        mqContextDisk.computeIfAbsent(
            Thread.currentThread().getName(),
            s -> {
              try {
                ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://1");
                ClientSessionFactory factory = serverLocator.createSessionFactory();
                ClientSession session = factory.createSession();
                ClientProducer producer = session.createProducer("diskBuffer");
                session.start();
                return new Pair<>(session, producer);
              } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
              }
              return null;
            });

    ClientSession session = mqCtx._1;
    ClientProducer producer = mqCtx._2;
    try {
      ClientMessage message = session.createMessage(true);
      message.writeBodyBufferString(strPoint);
      producer.send(message);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}

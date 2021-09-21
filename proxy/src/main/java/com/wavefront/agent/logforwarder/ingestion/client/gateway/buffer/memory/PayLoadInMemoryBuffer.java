package com.wavefront.agent.logforwarder.ingestion.client.gateway.buffer.memory;


import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;

import com.wavefront.agent.logforwarder.constants.LogForwarderConstants;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.metrics.MetricsService;
import com.wavefront.agent.logforwarder.ingestion.constants.IngestConstants;
import com.wavefront.agent.logforwarder.ingestion.processors.Processor;
import com.wavefront.agent.logforwarder.ingestion.processors.config.ComponentConfig;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.EventPayload;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.StructureFactory;
import com.wavefront.agent.logforwarder.ingestion.util.LogForwarderUtils;
import com.wavefront.agent.logforwarder.ingestion.util.RestApiSourcesMetricsUtil;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import io.dropwizard.metrics5.Histogram;
import io.dropwizard.metrics5.MetricRegistry;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 8/25/21 7:50 PM
 */
public class PayLoadInMemoryBuffer {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private BlockingQueue<EventPayload> inMemoryEventPayloadBuffer;

  private Histogram processingTimeTaken;
  private Histogram totalTimeTaken;
  private Histogram pollTimeTaken;
  private Histogram parseTimeTaken;
  private Histogram queueSize;
  private Histogram messagesInRequest;

  private ComponentConfig component;
  private int noOfWorkers = 1;

  public PayLoadInMemoryBuffer(ComponentConfig component) {
    this.component = component;
    if (LogForwarderConfigProperties.logForwarderArgs != null &&
        LogForwarderConfigProperties.logForwarderArgs.noOfWorkerThreads != null) {
      noOfWorkers = LogForwarderConfigProperties.logForwarderArgs.noOfWorkerThreads;
    }
    String process = MetricRegistry.name(getClass().getName(), component.component, "process" +
        ".timetaken-in-millis").getKey();
    processingTimeTaken = MetricsService
        .getInstance()
        .getHistogram(process);
    String total = MetricRegistry.name(getClass().getName(), component.component,
        "total.timetaken-in-millis").getKey();
    totalTimeTaken = MetricsService
        .getInstance()
        .getHistogram(total);
    String poll = MetricRegistry.name(getClass().getName(), component.component,
        "poll.timetaken-in-millis").getKey();
    pollTimeTaken = MetricsService
        .getInstance()
        .getHistogram(poll);
    String parse = MetricRegistry.name(getClass().getName(), component.component,
        "parse.timetaken-in-millis").getKey();
    parseTimeTaken = MetricsService
        .getInstance()
        .getHistogram(parse);
    String queue = MetricRegistry.name(getClass().getName(), component.component,
        "queue-size").getKey();
    queueSize = MetricsService
        .getInstance()
        .getHistogram(queue);

    messagesInRequest = MetricsService
        .getInstance()
        .getHistogram(MetricRegistry
            .name(getClass().getSimpleName(), component.component, "messages-in-request").
                getKey());

    inMemoryEventPayloadBuffer = new ArrayBlockingQueue<>(getBufferSize());
  }


  /**
   * add the event-payload to the queue
   * <p>
   * this method will call {@link java.util.concurrent.BlockingQueue#add(Object)}
   * if the queue is full, add method will throw {@link IllegalStateException}
   * this will be catched and false is returned
   *
   * @param payload event payload, not null
   * @return true if the event payload is added to the queue, false otherwise
   */
  public boolean addEventPayloadToQueue(EventPayload payload) {
    boolean isAddSuccessful = false;
    try {
      isAddSuccessful = inMemoryEventPayloadBuffer.add(payload);
      queueSize.update(inMemoryEventPayloadBuffer.size());
    } catch (IllegalStateException e) {
      logger.debug("exception when adding event payload in the inMemoryEventPayloadBuffer", e);
//      SystemAlertUtils.updateMessagesDroppedMetric(payload.batch.size());TODO
      MetricsService.getInstance()
          .getMeter("cfapi-enqueue-failed-blobs-" + component.component).mark();
      MetricsService.getInstance()
          .getMeter("cfapi-enqueue-failed-msgs-" + component.component).mark(payload.batch.size());
    }
    return isAddSuccessful;
  }

  /**
   * 1.if event payload processor is null, return
   * 2.else, create a daemon thread that does the following
   * a. block on the in-memory buffer until there is an event
   * b. wait until the minimum number of messages
   * is reached
   * c. execute the eventPayloadProcessor's sendEventPayload method
   * d. generate the required metrics
   * e. go to step `a`
   */
  public void dequeueMessagesAndSend() {
    Runnable runnable = () -> {
      while (true) {
        long startTime = System.currentTimeMillis();
        try {
          EventPayload eventPayload = getEventPayloadFromQueue();

          LogForwarderUtils.addForwarderIdIfNeeded(eventPayload);
          LogForwarderUtils.populateSource(eventPayload);

          long processStart = System.currentTimeMillis();
          for (Processor processor : component.processors) {
            if (LogForwarderConfigProperties.logForwarderArgs != null &&
                Boolean.TRUE.equals(LogForwarderConfigProperties.logForwarderArgs.fakeProcess)) {
              continue;
            }
            eventPayload = processor.process(eventPayload);
          }
          processingTimeTaken.update(System.currentTimeMillis() - processStart);
        } catch (Throwable e) {
          logger.error("Error during dequeue  : " + e.getMessage(), e);
          MetricsService.getInstance()
              .getMeter("rest-api-buffer-processing-failures-" + component.component).mark();
        } finally {
          totalTimeTaken.update((System.currentTimeMillis() - startTime));
        }
      }
    };
    for (int i = 0; i < noOfWorkers; i++) {
      Thread inMemoryBufferDequeueThread = new Thread(runnable);
      inMemoryBufferDequeueThread.setDaemon(true);
      inMemoryBufferDequeueThread.start();
    }
  }

  /**
   * 1. block on the in-memory buffer until there is an event
   * 2. wait until the minimum number of messages
   * is reached
   * 3. return with the event payload that contain all the messages dequeued from the queue
   *
   * @return event payload
   * @throws Exception
   */
  private EventPayload getEventPayloadFromQueue() throws Exception {
    long queuePollStartedTime = System.currentTimeMillis();
    EventPayload payload = inMemoryEventPayloadBuffer.take();
    pollTimeTaken.update(System.currentTimeMillis() - queuePollStartedTime);
    parsePayload(payload);

    while (payload.batch.size() < LogForwarderConfigProperties.minMsgsInBlob) {
      long currentTimeMillis = System.currentTimeMillis();
      long timeElapsedSincePollStart = currentTimeMillis - queuePollStartedTime;
            /*  if messages are getting polled from the queue,
            for more than {@link LogForwarderConstants#QUEUE_DEQUEUE_TIME_ELAPSED_SECS} break
            from the loop and return the data. this will make sure that events go near real time to the cloud */
      if (timeElapsedSincePollStart > (LogForwarderConstants.QUEUE_DEQUEUE_TIME_ELAPSED_SECS * 1000)) {
        break;
      }
      long pollTime = System.currentTimeMillis();
      EventPayload nextPayload =
          inMemoryEventPayloadBuffer.poll(LogForwarderConstants.QUEUE_POLL_TIMEOUT_SECS, TimeUnit.SECONDS);
      pollTimeTaken.update(System.currentTimeMillis() - pollTime);

      if (nextPayload == null) {
        break;
      } else {
        parsePayload(nextPayload);
        payload.batch.addAll(nextPayload.batch);
      }
    }
    return payload;
  }

  private void parsePayload(EventPayload eventPayload) {
    if (MapUtils.isNotEmpty(eventPayload.requestHeaders)
        && eventPayload.requestHeaders.get(IngestConstants.STRUCTURE_HEADER) != null) {
      String structure = eventPayload.requestHeaders.get(IngestConstants.STRUCTURE_HEADER);
      StructureFactory.Structure structureObj = StructureFactory.Structure.valueOf(structure);
      long startTime = System.currentTimeMillis();

      if (LogForwarderConfigProperties.logForwarderArgs != null &&
          Boolean.TRUE.equals(LogForwarderConfigProperties.logForwarderArgs.fakeParse)) {
        parseTimeTaken.update(System.currentTimeMillis() - startTime);
        return;
      }

      LogForwarderUtils.parseJSON(eventPayload, structureObj);
      messagesInRequest.update(eventPayload.batch.size());
      parseTimeTaken.update(System.currentTimeMillis() - startTime);
      eventPayload.requestHeaders.remove(IngestConstants.STRUCTURE_HEADER);

      RestApiSourcesMetricsUtil.updateMessageReceivedFromSource(structureObj,
          eventPayload.requestHeaders.get(LogForwarderUtils.REFERER_HEADER), eventPayload.batch.size());
    }
  }

  private int getBufferSize() {
    int bufferSize = LogForwarderConfigProperties.inMemoryBufferSize;
    int restApiQueueScale = 0;
    if (LogForwarderConfigProperties.logForwarderArgs != null &&
        LogForwarderConfigProperties.logForwarderArgs.restApiQueueScale != null) {
      restApiQueueScale = LogForwarderConfigProperties.logForwarderArgs.restApiQueueScale;
      if (restApiQueueScale > 0) {
        bufferSize = bufferSize * restApiQueueScale;
      }
    }
    logger.info("Rest API queue scale:{}, Buffer size:{}", restApiQueueScale, bufferSize);
    return bufferSize;
  }
}

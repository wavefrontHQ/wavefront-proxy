package com.wavefront.agent.queueing;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.metrics.ExpectedAgentMetric;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An AmazonSQS implementation of {@link TaskQueueFactory}
 *
 * @author mike@wavefront.com
 */
public class SQSQueueFactoryImpl implements TaskQueueFactory {
  private static final Logger logger =
      Logger.getLogger(SQSQueueFactoryImpl.class.getCanonicalName());

  private final Map<HandlerKey, Map<Integer, TaskQueue<?>>> taskQueues = new ConcurrentHashMap<>();

  private final String queueNameTemplate;
  private final String region;
  private final String queueId;
  private final boolean purgeBuffer;
  private final Map<String, String> queues = new ConcurrentHashMap<>();
  private final AmazonSQS client;

  public SQSQueueFactoryImpl(String template, String region, String queueId, boolean purgeBuffer) {
    this.queueNameTemplate = template;
    this.region = region;
    this.purgeBuffer = purgeBuffer;
    this.queueId = queueId;
    this.client = AmazonSQSClientBuilder.standard().withRegion(region).build();
  }

  @Override
  public <T extends DataSubmissionTask<T>> TaskQueue<T> getTaskQueue(@Nonnull HandlerKey key, int threadNum) {
    // noinspection unchecked
    TaskQueue<T> taskQueue = (TaskQueue<T>) taskQueues.computeIfAbsent(key, x -> new TreeMap<>()).
        computeIfAbsent(threadNum, x -> createTaskQueue(key, threadNum));
    return taskQueue;
  }

  private <T extends DataSubmissionTask<T>> TaskQueue<T> createTaskQueue(
      @Nonnull HandlerKey handlerKey, int threadNum) {
    if (purgeBuffer) {
      logger.warning("--purgeBuffer is set but purging buffers is not supported on SQS implementation");
    }

    final String queueName = getQueueName(handlerKey);
    String queueUrl = queues.computeIfAbsent(queueName, x -> getOrCreateQueue(queueName));
    if (StringUtils.isNotBlank(queueUrl)) {
      return new SQSSubmissionQueue<>(queueUrl, AmazonSQSClientBuilder.standard().withRegion(this.region).build(),
          new RetryTaskConverter<T>(handlerKey.getHandle(),
              RetryTaskConverter.CompressionType.LZ4),
          handlerKey.getHandle(), handlerKey.getEntityType(), threadNum);
    }
    return new TaskQueueStub<>();
  }

  @VisibleForTesting
  public String getQueueName(HandlerKey handlerKey) {
    String queueName = queueNameTemplate.
        replace("{{id}}", this.queueId).
        replace("{{entity}}",  handlerKey.getEntityType().toString()).
        replace("{{port}}", handlerKey.getHandle());
    return queueName;
  }

  private String getOrCreateQueue(String queueName) {
    String queueUrl = queues.getOrDefault(queueName, "");
    if (StringUtils.isNotBlank(queueUrl)) return queueUrl;
    try {
      GetQueueUrlResult queueUrlResult = client.getQueueUrl(new GetQueueUrlRequest().withQueueName(queueName));
      queueUrl = queueUrlResult.getQueueUrl();
    } catch (QueueDoesNotExistException e) {
      logger.info("Queue " + queueName + " does not exist...creating for first time");
    } catch (AmazonClientException e) {
      logger.log(Level.SEVERE, "Unable to lookup queue by name in aws " + queueName, e);
    }
    try {
      if (StringUtils.isBlank(queueUrl)) {
        CreateQueueRequest request = new CreateQueueRequest();
        request.
            addAttributesEntry(QueueAttributeName.MessageRetentionPeriod.toString(), "1209600").
            addAttributesEntry(QueueAttributeName.ReceiveMessageWaitTimeSeconds.toString(), "20").
            addAttributesEntry(QueueAttributeName.VisibilityTimeout.toString(), "60").
            setQueueName(queueName);
        CreateQueueResult result = client.createQueue(request);
        queueUrl = result.getQueueUrl();
        queues.put(queueName, queueUrl);
      }
    } catch (AmazonClientException e) {
      logger.log(Level.SEVERE, "Error creating queue in AWS " + queueName, e);
    }

    return queueUrl;
  }

  public static boolean isValidSQSTemplate(String template) {
    return template.contains("{{id}}") && template.contains("{{entity}}") && template.contains("{{port}}");
  }
}

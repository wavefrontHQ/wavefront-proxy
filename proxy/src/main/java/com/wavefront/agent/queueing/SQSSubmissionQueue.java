package com.wavefront.agent.queueing;

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import static javax.xml.bind.DatatypeConverter.parseBase64Binary;
import static javax.xml.bind.DatatypeConverter.printBase64Binary;

/**
 * Implements proxy-specific queue interface as a wrapper over {@link AmazonSQS}
 *
 * @param <T> type of objects stored.
 *
 * @author mike@wavefront.com
 */
public class SQSSubmissionQueue<T extends DataSubmissionTask<T>>
    implements TaskQueue<T> {
  private static final Logger log = Logger.getLogger(SQSSubmissionQueue.class.getCanonicalName());

  private final String queueUrl;
  private final TaskConverter<T> converter;
  private AtomicLong currentWeight = null;

  @Nullable
  private final String handle;
  private final String entityName;
  private final AmazonSQS sqsClient;
  private final int threadNum;

  private final Counter tasksAddedCounter;
  private final Counter itemsAddedCounter;
  private final Counter tasksRemovedCounter;
  private final Counter itemsRemovedCounter;

  private String messageHandle = null;
  private T head = null;

  // maintain a fair lock on the queue
  private final ReentrantLock queueLock = new ReentrantLock(true);

  public SQSSubmissionQueue(String queueUrl,
                            AmazonSQS sqsClient,
                            TaskConverter<T> converter,
                            @Nullable String handle,
                            @Nullable ReportableEntityType entityType,
                            int threadNum) {
    this.queueUrl = queueUrl;
    this.converter = converter;
    this.handle = handle;
    this.entityName = entityType == null ? "points" : entityType.toString();
    this.sqsClient = sqsClient;
    this.threadNum = threadNum;

    this.tasksAddedCounter = Metrics.newCounter(new TaggedMetricName("buffer.sqs", "task-added",
        "port", handle, "queueUrl", queueUrl));
    this.itemsAddedCounter = Metrics.newCounter(new TaggedMetricName("buffer.sqs", entityName +
        "-added", "port", handle, "queueUrl", queueUrl));
    this.tasksRemovedCounter = Metrics.newCounter(new TaggedMetricName("buffer.sqs", "task-removed",
        "port", handle, "queueUrl", queueUrl));
    this.itemsRemovedCounter = Metrics.newCounter(new TaggedMetricName("buffer.sqs", entityName +
        "-removed", "port", handle, "queueUrl", queueUrl));
  }

  @Override
  public T peek() {
    if (this.head != null) return head;
    queueLock.lock();
    try {
      ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest(this.queueUrl);
      receiveRequest.setMaxNumberOfMessages(1);
      receiveRequest.setWaitTimeSeconds(1);
      ReceiveMessageResult result = sqsClient.receiveMessage(receiveRequest);
      List<Message> messages = result.getMessages();
      if (messages.size() <= 0) {
        return null;
      }
      Message message = messages.get(0);
      byte[] messageBytes = parseBase64Binary(message.getBody());
      messageHandle = message.getReceiptHandle();
      head = converter.from(messageBytes);
      return head;
    } catch (AmazonClientException e) {
      log.log(Level.SEVERE, "AmazonClientException while trying to peek the queues, ", e.getMessage());
    } finally {
      queueLock.unlock();
    }
    return null;
  }

  @Override
  public void add(@Nonnull T t) throws IOException {
    queueLock.lock();
    try {
      SendMessageRequest request = new SendMessageRequest();
      String contents = encodeMessageForDelivery(t);
      request.setMessageBody(contents);
      request.setQueueUrl(queueUrl);
      sqsClient.sendMessage(request);
      if (currentWeight != null) {
        currentWeight.addAndGet(t.weight());
      }
      tasksAddedCounter.inc();
      itemsAddedCounter.inc(t.weight());
    } catch (AmazonClientException e) {
      throw new IOException("AmazonClientException adding messages onto the queue", e);
    } finally {
      queueLock.unlock();
    }
  }

  @VisibleForTesting
  public String encodeMessageForDelivery(T t) throws IOException {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      converter.toStream(t, os);
      byte[] contents = os.toByteArray();
      return printBase64Binary(contents);
    }
  }

  @Override
  public void remove() {
    queueLock.lock();
    try {
      // We have no head, do not remove
      if (StringUtils.isBlank(messageHandle) || head == null) {
        return;
      }
      int taskSize = head.weight();
      if (currentWeight != null) {
        currentWeight.getAndUpdate(x -> x > taskSize ? x - taskSize : 0);
      }
      DeleteMessageRequest deleteRequest = new DeleteMessageRequest(this.queueUrl, this.messageHandle);
      sqsClient.deleteMessage(deleteRequest);
      this.head = null;
      this.messageHandle = null;
      initializeTracking();
      tasksRemovedCounter.inc();
      itemsRemovedCounter.inc(taskSize);
    } catch (AmazonClientException e) {
      Metrics.newCounter(new TaggedMetricName("buffer.sqs", "failures", "port",
          handle, "queueUrl", queueUrl)).inc();
      log.log(Level.SEVERE, "AmazonClientException removing from the queue", e);
    } finally {
      queueLock.unlock();
    }
  }

  @Override
  public void clear() {
    queueLock.lock();
    try {
      sqsClient.purgeQueue(new PurgeQueueRequest(this.queueUrl));
    } catch (AmazonClientException e) {
      Metrics.newCounter(new TaggedMetricName("buffer.sqs", "failures", "port",
          handle, "queueUrl", queueUrl)).inc();
      log.log(Level.SEVERE, "AmazonClientException clearing the queue", e);
    } finally {
      queueLock.unlock();
    }
  }

  @Override
  public int size() {
    int queueSize = 0;
    try {
      GetQueueAttributesRequest request = new GetQueueAttributesRequest(this.queueUrl);
      request.withAttributeNames(QueueAttributeName.ApproximateNumberOfMessages);
      GetQueueAttributesResult result = sqsClient.getQueueAttributes(request);
      queueSize = Integer.parseInt(result.getAttributes().getOrDefault(
          QueueAttributeName.ApproximateNumberOfMessages.toString(), "0"));
    } catch (AmazonClientException e) {
      log.log(Level.SEVERE, "Unable to obtain ApproximateNumberOfMessages from queue", e);
    } catch (NumberFormatException e) {
      log.log(Level.SEVERE, "Value returned for approximate number of messages is not a valid number", e);
    }
    return queueSize;
  }

  @Override
  public void close() {
    // Nothing to close
  }

  @Nullable
  @Override
  public Long weight() {
    return currentWeight == null ? null : currentWeight.get();
  }

  @Nullable
  @Override
  public Long getAvailableBytes() {
    throw new UnsupportedOperationException("Cannot obtain total bytes from SQS queue, consider using size instead");
  }

  private synchronized void initializeTracking() {
    if (currentWeight == null) {
      currentWeight = new AtomicLong(0);
    } else {
      currentWeight.set(0);
    }
  }
}

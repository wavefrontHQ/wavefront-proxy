package com.wavefront.agent.queueing;

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
import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.common.Utils;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
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
public class SQSSubmissionQueue<T extends DataSubmissionTask<T>> implements TaskQueue<T> {
  private static final Logger log = Logger.getLogger(SQSSubmissionQueue.class.getCanonicalName());

  private final String queueUrl;
  private final TaskConverter<T> converter;

  private final AmazonSQS sqsClient;

  private volatile String messageHandle = null;
  private volatile T head = null;

  /**
   * @param queueUrl   The FQDN of the SQS Queue
   * @param sqsClient  The {@link AmazonSQS} client.
   * @param converter  The {@link TaskQueue<T>} for converting tasks into and from the Queue
   */
  public SQSSubmissionQueue(String queueUrl,
                            AmazonSQS sqsClient,
                            TaskConverter<T> converter) {
    this.queueUrl = queueUrl;
    this.converter = converter;
    this.sqsClient = sqsClient;
  }

  @Override
  public T peek() {
    try {
      if (this.head != null) return head;
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
      head = converter.fromBytes(messageBytes);
      return head;
    } catch (IOException e) {
      throw Utils.<Error>throwAny(e);
    } catch (AmazonClientException e) {
      throw Utils.<Error>throwAny(
          new IOException("AmazonClientException while trying to peek the queues, ", e));
    }
  }

  @Override
  public void add(@Nonnull T t) throws IOException {
    try {
      SendMessageRequest request = new SendMessageRequest();
      String contents = encodeMessageForDelivery(t);
      request.setMessageBody(contents);
      request.setQueueUrl(queueUrl);
      sqsClient.sendMessage(request);
    } catch (AmazonClientException e) {
      throw new IOException("AmazonClientException adding messages onto the queue", e);
    }
  }

  @VisibleForTesting
  public String encodeMessageForDelivery(T t) throws IOException {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      converter.serializeToStream(t, os);
      byte[] contents = os.toByteArray();
      return printBase64Binary(contents);
    }
  }

  @Override
  public void remove() throws IOException {
    try {
      // We have no head, do not remove
      if (StringUtils.isBlank(messageHandle) || head == null) {
        return;
      }
      int taskSize = head.weight();
      DeleteMessageRequest deleteRequest = new DeleteMessageRequest(this.queueUrl,
          this.messageHandle);
      sqsClient.deleteMessage(deleteRequest);
      this.head = null;
      this.messageHandle = null;
    } catch (AmazonClientException e) {
      throw new IOException("AmazonClientException removing from the queue", e);
    }
  }

  @Override
  public void clear() throws IOException {
    try {
      sqsClient.purgeQueue(new PurgeQueueRequest(this.queueUrl));
    } catch (AmazonClientException e) {
      throw new IOException("AmazonClientException clearing the queue", e);
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
      log.log(Level.SEVERE, "Value returned for approximate number of messages is not a " +
          "valid number", e);
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
    return null;
  }

  @Nullable
  @Override
  public Long getAvailableBytes() {
    throw new UnsupportedOperationException("Cannot obtain total bytes from SQS queue, " +
        "consider using size instead");
  }

  @Override
  public String getName() {
    return queueUrl;
  }

  @NotNull
  @Override
  public Iterator<T> iterator() {
    throw new UnsupportedOperationException("iterator() is not supported on a SQS queue");
  }
}

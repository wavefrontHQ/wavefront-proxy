package com.wavefront.agent.core.buffers;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.data.EntityRateLimiter;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;

public class SQSBuffer implements Buffer {
  private static final Logger log = Logger.getLogger(SQSBuffer.class.getCanonicalName());

  private final String template;
  private final AmazonSQS client;
  private final Map<String, String> queuesUrls = new HashMap<>();
  private final String visibilityTimeOut;

  public SQSBuffer(SQSBufferConfig cfg) {
    this.template = cfg.template;
    this.client = AmazonSQSClientBuilder.standard().withRegion(cfg.region).build();
    visibilityTimeOut = String.valueOf(cfg.vto);
  }

  public String getName() {
    return "SQS";
  }

  @Override
  public int getPriority() {
    return Thread.NORM_PRIORITY;
  }

  @Override
  public void registerNewQueueInfo(QueueInfo queue) {
    String queueName = queue.getName();
    String queueUrl = null;

    try {
      GetQueueUrlResult queueUrlResult =
          client.getQueueUrl(new GetQueueUrlRequest().withQueueName(queueName));
      queueUrl = queueUrlResult.getQueueUrl();
    } catch (QueueDoesNotExistException e) {
      log.info("Queue " + queueName + " does not exist...creating for first time");
    } catch (AmazonClientException e) {
      log.log(Level.SEVERE, "Unable to lookup queue by name in aws " + queueName, e);
    }

    if (queueUrl == null) {
      try {
        CreateQueueRequest request = new CreateQueueRequest();
        request
            .addAttributesEntry(
                QueueAttributeName.MessageRetentionPeriod.toString(), "1209600") // 14 days
            .addAttributesEntry(QueueAttributeName.ReceiveMessageWaitTimeSeconds.toString(), "20")
            .addAttributesEntry(
                QueueAttributeName.VisibilityTimeout.toString(), visibilityTimeOut) // 1 minute
            .setQueueName(queueName);
        CreateQueueResult result = client.createQueue(request);
        queueUrl = result.getQueueUrl();
        log.info("queue " + queueName + " created. url:" + queueUrl);
      } catch (AmazonClientException e) {
        log.log(Level.SEVERE, "Error creating queue in AWS " + queueName, e);
      }
    }

    queuesUrls.put(queue.getName(), queueUrl);
  }

  @Override
  public void onMsgBatch(
      QueueInfo queue, int idx, int batchSize, EntityRateLimiter rateLimiter, OnMsgFunction func) {

    String queueUrl = queuesUrls.get(queue.getName());
    long start = System.currentTimeMillis();
    List<String> batch = new ArrayList<>(batchSize);
    List<Message> messagesToDelete = new ArrayList<>();
    boolean done = false;
    while ((batch.size() < batchSize) && !done && ((System.currentTimeMillis() - start) < 1000)) {
      ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest(queueUrl);
      receiveRequest.setMaxNumberOfMessages(1);
      receiveRequest.setWaitTimeSeconds(1);
      ReceiveMessageResult result = client.receiveMessage(receiveRequest);
      List<Message> messages = result.getMessages();
      if (messages.size() == 1) {
        List<String> points = Arrays.asList(messages.get(0).getBody().split("\n"));
        batch.addAll(points);
        messagesToDelete.addAll(messages);
      } else {
        done = true;
      }
    }

    try {
      if (batch.size() > 0) {
        func.run(batch);
      }
      messagesToDelete.forEach(
          message -> {
            client.deleteMessage(queueUrl, message.getReceiptHandle());
          });
    } catch (Exception e) {
      log.log(Level.SEVERE, e.getMessage());
      if (log.isLoggable(Level.FINER)) {
        log.log(Level.SEVERE, "error", e);
      }
    }
  }

  @Override
  public void sendPoints(String queue, List<String> points) throws ActiveMQAddressFullException {
    try {
      SendMessageRequest request = new SendMessageRequest();
      request.setMessageBody(String.join("\n", points));
      request.setQueueUrl(queuesUrls.get(queue));
      client.sendMessage(request);
    } catch (AmazonClientException e) {
      throw new RuntimeException("Error sending message to queue '" + queue + "'", e);
    }
  }

  public void truncateQueue(String queue) {
    try {
      client.purgeQueue(new PurgeQueueRequest(queuesUrls.get(queue)));
    } catch (AmazonClientException e) {
      log.log(Level.SEVERE, "Error truncating queue '" + queue + "'", e);
    }
  }
}

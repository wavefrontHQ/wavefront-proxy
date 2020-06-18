package com.wavefront.agent.queueing;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.common.collect.ImmutableList;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.data.DefaultEntityPropertiesForTesting;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.data.ReportableEntityType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class SQSSubmissionQueueTest<T extends DataSubmissionTask<T>> {
  private final String queueUrl = "https://amazonsqs.some.queue";
  private final TaskConverter.CompressionType compressionType;
  private AmazonSQS client = createMock(AmazonSQS.class);
  private final T expectedTask;
  private final RetryTaskConverter<T> converter;
  private final AtomicLong time = new AtomicLong(77777);

  public SQSSubmissionQueueTest(TaskConverter.CompressionType compressionType, RetryTaskConverter<T> converter, T task) {
    this.compressionType = compressionType;
    this.converter = converter;
    this.expectedTask = task;
    System.out.println("compression type: " + compressionType);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> scenarios() {
    Collection<Object[]> scenarios = new ArrayList<>();
    for (TaskConverter.CompressionType type : TaskConverter.CompressionType.values()) {
      RetryTaskConverter<LineDelimitedDataSubmissionTask> converter = new RetryTaskConverter<>("2878", type);
      LineDelimitedDataSubmissionTask task = converter.from("WF\u0001\u0001{\"__CLASS\":\"com.wavefront.agent.data.LineDelimitedDataSubmissionTask\",\"enqueuedTimeMillis\":77777,\"attempts\":0,\"serverErrors\":0,\"handle\":\"2878\",\"entityType\":\"POINT\",\"format\":\"wavefront\",\"payload\":[\"java.util.ArrayList\",[\"item1\",\"item2\",\"item3\"]],\"enqueuedMillis\":77777}".getBytes());
      scenarios.add(new Object[]{type, converter, task});
    }
    return scenarios;
  }

  @Test
  public void testTaskPurge() {
    // noinspection unchecked
    SQSSubmissionQueue<LineDelimitedDataSubmissionTask> queue = getTaskQueue();
    reset(client);
    expect(client.purgeQueue(new PurgeQueueRequest(queueUrl))).andReturn(null);
    replay(client);
    queue.clear();
    queue.close(); // note this does not actually do anything...
  }

  @Test
  public void testTaskRead() throws IOException {
    // noinspection unchecked
    SQSSubmissionQueue<LineDelimitedDataSubmissionTask> queue = getTaskQueue();
    UUID proxyId = UUID.randomUUID();
    LineDelimitedDataSubmissionTask task = new LineDelimitedDataSubmissionTask(null, proxyId,
        new DefaultEntityPropertiesForTesting(), queue, "wavefront", ReportableEntityType.POINT,
        "2878", ImmutableList.of("item1", "item2", "item3"), time::get);

    expect(client.sendMessage(
        new SendMessageRequest(
            queueUrl,
            queue.encodeMessageForDelivery((LineDelimitedDataSubmissionTask) this.expectedTask)))).
        andReturn(null);
    replay(client);
    task.enqueue(QueueingReason.RETRY);

    reset(client);
    ReceiveMessageRequest msgRequest = new ReceiveMessageRequest().
        withMaxNumberOfMessages(1).
        withWaitTimeSeconds(1).
        withQueueUrl(queueUrl);
    ReceiveMessageResult msgResult = new ReceiveMessageResult().
        withMessages(new Message().withBody(queue.encodeMessageForDelivery(task)).withReceiptHandle("handle1"));

    expect(client.receiveMessage(msgRequest)).andReturn(msgResult);
    replay(client);
    LineDelimitedDataSubmissionTask readTask = queue.peek();
    assertEquals(task.payload(), readTask.payload());
    assertEquals(77777, readTask.getEnqueuedMillis());
  }

  private SQSSubmissionQueue getTaskQueue() {
    //noinspection unchecked
    return new SQSSubmissionQueue(queueUrl, client, converter, "2878", null, 0);
  }
}

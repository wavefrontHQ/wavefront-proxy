package com.wavefront.agent.queueing;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.data.DefaultEntityPropertiesForTesting;
import com.wavefront.agent.data.EventDataSubmissionTask;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.Event;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import wavefront.report.ReportEvent;

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

/**
 * @author mike@wavefront.com
 */
@RunWith(Parameterized.class)
public class SQSSubmissionQueueTest<T extends DataSubmissionTask<T>> {
  private final String queueUrl = "https://amazonsqs.some.queue";
  private AmazonSQS client = createMock(AmazonSQS.class);
  private final T expectedTask;
  private final RetryTaskConverter<T> converter;
  private final AtomicLong time = new AtomicLong(77777);

  public SQSSubmissionQueueTest(TaskConverter.CompressionType compressionType, RetryTaskConverter<T> converter, T task) {
    this.converter = converter;
    this.expectedTask = task;
    System.out.println(task.getClass().getSimpleName() + " compression type: " + compressionType);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> scenarios() {
    Collection<Object[]> scenarios = new ArrayList<>();
    for (TaskConverter.CompressionType type : TaskConverter.CompressionType.values()) {
      RetryTaskConverter<LineDelimitedDataSubmissionTask> converter = new RetryTaskConverter<>("2878", type);
      LineDelimitedDataSubmissionTask task = converter.fromBytes("WF\u0001\u0001{\"__CLASS\":\"com.wavefront.agent.data.LineDelimitedDataSubmissionTask\",\"enqueuedTimeMillis\":77777,\"attempts\":0,\"serverErrors\":0,\"handle\":\"2878\",\"entityType\":\"POINT\",\"format\":\"wavefront\",\"payload\":[\"java.util.ArrayList\",[\"item1\",\"item2\",\"item3\"]],\"enqueuedMillis\":77777}".getBytes());
      scenarios.add(new Object[]{type, converter, task});
    }
    for (TaskConverter.CompressionType type : TaskConverter.CompressionType.values()) {
      RetryTaskConverter<EventDataSubmissionTask> converter = new RetryTaskConverter<>("2878", type);
      EventDataSubmissionTask task = converter.fromBytes("WF\u0001\u0001{\"__CLASS\":\"com.wavefront.agent.data.EventDataSubmissionTask\",\"enqueuedTimeMillis\":77777,\"attempts\":0,\"serverErrors\":0,\"handle\":\"2878\",\"entityType\":\"EVENT\",\"events\":[\"java.util.ArrayList\",[{\"name\":\"Event name for testing\",\"startTime\":77777000,\"endTime\":77777001,\"annotations\":[\"java.util.HashMap\",{\"severity\":\"INFO\"}],\"dimensions\":[\"java.util.HashMap\",{\"multi\":[\"java.util.ArrayList\",[\"bar\",\"baz\"]]}],\"hosts\":[\"java.util.ArrayList\",[\"host1\",\"host2\"]],\"tags\":[\"java.util.ArrayList\",[\"tag1\"]]}]],\"enqueuedMillis\":77777}".getBytes());
      scenarios.add(new Object[]{type, converter, task});
    }
    return scenarios;
  }

  @Test
  public void testTaskRead() throws IOException {
    SQSSubmissionQueue queue = getTaskQueue();
    UUID proxyId = UUID.randomUUID();
    DataSubmissionTask<? extends DataSubmissionTask<?>> task;
    if (this.expectedTask instanceof LineDelimitedDataSubmissionTask) {
      task = new LineDelimitedDataSubmissionTask(null, proxyId,
          new DefaultEntityPropertiesForTesting(), queue, "wavefront", ReportableEntityType.POINT,
          "2878", ImmutableList.of("item1", "item2", "item3"), time::get);
    } else if (this.expectedTask instanceof EventDataSubmissionTask) {
      task = new EventDataSubmissionTask(null, proxyId,
          new DefaultEntityPropertiesForTesting(), queue, "2878",
          ImmutableList.of(
              new Event(ReportEvent.newBuilder().
                  setStartTime(time.get() * 1000).
                  setEndTime(time.get() * 1000 + 1).
                  setName("Event name for testing").
                  setHosts(ImmutableList.of("host1", "host2")).
                  setDimensions(ImmutableMap.of("multi", ImmutableList.of("bar", "baz"))).
                  setAnnotations(ImmutableMap.of("severity", "INFO")).
                  setTags(ImmutableList.of("tag1")).
                  build())),
          time::get);
    } else {
      task = null;
    }
    expect(client.sendMessage(
        new SendMessageRequest(
            queueUrl,
            queue.encodeMessageForDelivery(this.expectedTask)))).
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
    if (this.expectedTask instanceof LineDelimitedDataSubmissionTask) {
      LineDelimitedDataSubmissionTask readTask = (LineDelimitedDataSubmissionTask) queue.peek();
      assertEquals(((LineDelimitedDataSubmissionTask)task).payload(), readTask.payload());
      assertEquals(((LineDelimitedDataSubmissionTask) this.expectedTask).payload(), readTask.payload());
      assertEquals(77777, readTask.getEnqueuedMillis());
    }
    if (this.expectedTask instanceof EventDataSubmissionTask) {
      EventDataSubmissionTask readTask = (EventDataSubmissionTask) queue.peek();
      assertEquals(((EventDataSubmissionTask)task).payload(), readTask.payload());
      assertEquals(((EventDataSubmissionTask) this.expectedTask).payload(), readTask.payload());
      assertEquals(77777, readTask.getEnqueuedMillis());
    }
  }

  private SQSSubmissionQueue<? extends T> getTaskQueue() {
    return new SQSSubmissionQueue<>(queueUrl, client, converter);
  }
}

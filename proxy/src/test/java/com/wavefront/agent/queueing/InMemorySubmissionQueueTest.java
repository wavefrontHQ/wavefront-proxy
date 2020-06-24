package com.wavefront.agent.queueing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.data.DefaultEntityPropertiesForTesting;
import com.wavefront.agent.data.EventDataSubmissionTask;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.data.SourceTagSubmissionTask;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.Event;
import com.wavefront.dto.SourceTag;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import wavefront.report.ReportEvent;
import wavefront.report.ReportSourceTag;
import wavefront.report.SourceOperationType;
import wavefront.report.SourceTagAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author mike@wavefront.com
 */
@RunWith(Parameterized.class)
public class InMemorySubmissionQueueTest<T extends DataSubmissionTask<T>> {
  private final T expectedTask;
  private final AtomicLong time = new AtomicLong(77777);

  public InMemorySubmissionQueueTest(TaskConverter.CompressionType compressionType, T task) {
    this.expectedTask = task;
    System.out.println(task.getClass().getSimpleName() + " compression type: " + compressionType);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> scenarios() {
    Collection<Object[]> scenarios = new ArrayList<>();
    for (TaskConverter.CompressionType type : TaskConverter.CompressionType.values()) {
      RetryTaskConverter<LineDelimitedDataSubmissionTask> converter = new RetryTaskConverter<>("2878", type);
      LineDelimitedDataSubmissionTask task = converter.fromBytes("WF\u0001\u0001{\"__CLASS\":\"com.wavefront.agent.data.LineDelimitedDataSubmissionTask\",\"enqueuedTimeMillis\":77777,\"attempts\":0,\"serverErrors\":0,\"handle\":\"2878\",\"entityType\":\"POINT\",\"format\":\"wavefront\",\"payload\":[\"java.util.ArrayList\",[\"item1\",\"item2\",\"item3\"]],\"enqueuedMillis\":77777}".getBytes());
      scenarios.add(new Object[]{type, task});
    }
    for (TaskConverter.CompressionType type : TaskConverter.CompressionType.values()) {
      RetryTaskConverter<EventDataSubmissionTask> converter = new RetryTaskConverter<>("2878", type);
      EventDataSubmissionTask task = converter.fromBytes("WF\u0001\u0001{\"__CLASS\":\"com.wavefront.agent.data.EventDataSubmissionTask\",\"enqueuedTimeMillis\":77777,\"attempts\":0,\"serverErrors\":0,\"handle\":\"2878\",\"entityType\":\"EVENT\",\"events\":[\"java.util.ArrayList\",[{\"name\":\"Event name for testing\",\"startTime\":77777000,\"endTime\":77777001,\"annotations\":[\"java.util.HashMap\",{\"severity\":\"INFO\"}],\"dimensions\":[\"java.util.HashMap\",{\"multi\":[\"java.util.ArrayList\",[\"bar\",\"baz\"]]}],\"hosts\":[\"java.util.ArrayList\",[\"host1\",\"host2\"]],\"tags\":[\"java.util.ArrayList\",[\"tag1\"]]}]],\"enqueuedMillis\":77777}".getBytes());
      scenarios.add(new Object[]{type, task});
    }
    for (TaskConverter.CompressionType type : TaskConverter.CompressionType.values()) {
      RetryTaskConverter<SourceTagSubmissionTask> converter = new RetryTaskConverter<>("2878", type);
      SourceTagSubmissionTask task = converter.fromBytes("WF\u0001\u0001{\"__CLASS\":\"com.wavefront.agent.data.SourceTagSubmissionTask\",\"enqueuedTimeMillis\":77777,\"attempts\":0,\"serverErrors\":0,\"handle\":\"2878\",\"entityType\":\"SOURCE_TAG\",\"limitRetries\":true,\"sourceTag\":{\"operation\":\"SOURCE_TAG\",\"action\":\"SAVE\",\"source\":\"testSource\",\"annotations\":[\"java.util.ArrayList\",[\"newtag1\",\"newtag2\"]]},\"enqueuedMillis\":77777}\n".getBytes());
      scenarios.add(new Object[]{type, task});
    }
    return scenarios;
  }

  @Test
  public void testTaskRead() {
    TaskQueue queue = new InMemorySubmissionQueue<>();
    UUID proxyId = UUID.randomUUID();
    DataSubmissionTask<? extends DataSubmissionTask<?>> task = null;
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
    } else if (this.expectedTask instanceof SourceTagSubmissionTask) {
      task = new SourceTagSubmissionTask(null,
          new DefaultEntityPropertiesForTesting(), queue, "2878",
          new SourceTag(
              ReportSourceTag.newBuilder().setOperation(SourceOperationType.SOURCE_TAG).
                  setAction(SourceTagAction.SAVE).setSource("testSource").
                  setAnnotations(ImmutableList.of("newtag1", "newtag2")).build()),
          time::get);
    }
    assertNotNull(task);
    task.enqueue(QueueingReason.RETRY);

    if (this.expectedTask instanceof LineDelimitedDataSubmissionTask) {
      LineDelimitedDataSubmissionTask readTask = (LineDelimitedDataSubmissionTask) queue.peek();
      assertNotNull(readTask);
      assertEquals(((LineDelimitedDataSubmissionTask) task).payload(), readTask.payload());
      assertEquals(((LineDelimitedDataSubmissionTask) this.expectedTask).payload(), readTask.payload());
      assertEquals(77777, readTask.getEnqueuedMillis());
    }
    if (this.expectedTask instanceof EventDataSubmissionTask) {
      EventDataSubmissionTask readTask = (EventDataSubmissionTask) queue.peek();
      assertNotNull(readTask);
      assertEquals(((EventDataSubmissionTask) task).payload(), readTask.payload());
      assertEquals(((EventDataSubmissionTask) this.expectedTask).payload(), readTask.payload());
      assertEquals(77777, readTask.getEnqueuedMillis());
    }
    if (this.expectedTask instanceof SourceTagSubmissionTask) {
      SourceTagSubmissionTask readTask = (SourceTagSubmissionTask) queue.peek();
      assertNotNull(readTask);
      assertEquals(((SourceTagSubmissionTask) task).payload(), readTask.payload());
      assertEquals(((SourceTagSubmissionTask) this.expectedTask).payload(), readTask.payload());
      assertEquals(77777, readTask.getEnqueuedMillis());
    }
  }
}

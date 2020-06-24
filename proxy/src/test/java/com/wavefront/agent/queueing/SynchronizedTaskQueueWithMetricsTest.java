package com.wavefront.agent.queueing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.squareup.tape2.QueueFile;
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
import wavefront.report.ReportEvent;
import wavefront.report.ReportSourceTag;
import wavefront.report.SourceOperationType;
import wavefront.report.SourceTagAction;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

/**
 * Tests object serialization.
 *
 * @author vasily@wavefront.com
 */
public class SynchronizedTaskQueueWithMetricsTest {

  @Test
  public void testLineDelimitedTask() throws Exception {
    AtomicLong time = new AtomicLong(77777);
    for (RetryTaskConverter.CompressionType type : RetryTaskConverter.CompressionType.values()) {
      System.out.println("LineDelimited task, compression type: " + type);
      File file = new File(File.createTempFile("proxyTestConverter", null).getPath() + ".queue");
      file.deleteOnExit();
      TaskQueue<LineDelimitedDataSubmissionTask> queue = getTaskQueue(file, type);
      queue.clear();
      UUID proxyId = UUID.randomUUID();
      LineDelimitedDataSubmissionTask task = new LineDelimitedDataSubmissionTask(null, proxyId,
          new DefaultEntityPropertiesForTesting(), queue, "wavefront", ReportableEntityType.POINT,
          "2878", ImmutableList.of("item1", "item2", "item3"), time::get);
      task.enqueue(QueueingReason.RETRY);
      queue.close();
      TaskQueue<LineDelimitedDataSubmissionTask> readQueue = getTaskQueue(file, type);
      LineDelimitedDataSubmissionTask readTask = readQueue.peek();
      assertEquals(task.payload(), readTask.payload());
      assertEquals(77777, readTask.getEnqueuedMillis());
    }
  }

  @Test
  public void testSourceTagTask() throws Exception {
    for (RetryTaskConverter.CompressionType type : RetryTaskConverter.CompressionType.values()) {
      System.out.println("SourceTag task, compression type: " + type);
      File file = new File(File.createTempFile("proxyTestConverter", null).getPath() + ".queue");
      file.deleteOnExit();
      TaskQueue<SourceTagSubmissionTask> queue = getTaskQueue(file, type);
      queue.clear();
      SourceTagSubmissionTask task = new SourceTagSubmissionTask(null,
          new DefaultEntityPropertiesForTesting(), queue, "2878",
          new SourceTag(
              ReportSourceTag.newBuilder().setOperation(SourceOperationType.SOURCE_TAG).
                  setAction(SourceTagAction.SAVE).setSource("testSource").
                  setAnnotations(ImmutableList.of("newtag1", "newtag2")).build()),
          () -> 77777L);
      task.enqueue(QueueingReason.RETRY);
      queue.close();
      TaskQueue<SourceTagSubmissionTask> readQueue = getTaskQueue(file, type);
      SourceTagSubmissionTask readTask = readQueue.peek();
      assertEquals(task.payload(), readTask.payload());
      assertEquals(77777, readTask.getEnqueuedMillis());
    }
  }

  @Test
  public void testEventTask() throws Exception {
    AtomicLong time = new AtomicLong(77777);
    for (RetryTaskConverter.CompressionType type : RetryTaskConverter.CompressionType.values()) {
      System.out.println("Event task, compression type: " + type);
      File file = new File(File.createTempFile("proxyTestConverter", null).getPath() + ".queue");
      file.deleteOnExit();
      TaskQueue<EventDataSubmissionTask> queue = getTaskQueue(file, type);
      queue.clear();
      UUID proxyId = UUID.randomUUID();
      EventDataSubmissionTask task = new EventDataSubmissionTask(null, proxyId,
          new DefaultEntityPropertiesForTesting(), queue, "2878",
          ImmutableList.of(new Event(ReportEvent.newBuilder().
              setStartTime(time.get() * 1000).
              setEndTime(time.get() * 1000 + 1).
              setName("Event name for testing").
              setHosts(ImmutableList.of("host1", "host2")).
              setDimensions(ImmutableMap.of("multi", ImmutableList.of("bar", "baz"))).
              setAnnotations(ImmutableMap.of("severity", "INFO")).
              setTags(ImmutableList.of("tag1")).
              build())),
          time::get);
      task.enqueue(QueueingReason.RETRY);
      queue.close();
      TaskQueue<EventDataSubmissionTask> readQueue = getTaskQueue(file, type);
      EventDataSubmissionTask readTask = readQueue.peek();
      assertEquals(task.payload(), readTask.payload());
      assertEquals(77777, readTask.getEnqueuedMillis());
    }
  }

  private <T extends DataSubmissionTask<T>> TaskQueue<T> getTaskQueue(
      File file, RetryTaskConverter.CompressionType compressionType) throws Exception {
    return new SynchronizedTaskQueueWithMetrics<>(new FileBasedTaskQueue<>(
        new QueueFile.Builder(file).build(),
        new RetryTaskConverter<T>("2878",
            compressionType)),
        null, null, null);
  }
}
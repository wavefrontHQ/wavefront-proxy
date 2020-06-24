package com.wavefront.agent.queueing;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.wavefront.agent.ProxyConfig;
import com.wavefront.agent.data.DefaultEntityPropertiesForTesting;
import com.wavefront.agent.data.EntityPropertiesFactory;
import com.wavefront.agent.data.EntityPropertiesFactoryImpl;
import com.wavefront.agent.data.EventDataSubmissionTask;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.data.SourceTagSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.Event;
import com.wavefront.dto.SourceTag;
import org.easymock.EasyMock;
import org.junit.Test;
import wavefront.report.ReportEvent;
import wavefront.report.ReportSourceTag;
import wavefront.report.SourceOperationType;
import wavefront.report.SourceTagAction;

import java.io.BufferedWriter;
import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author vasily@wavefront.com
 */
public class QueueExporterTest {

  @Test
  public void testQueueExporter() throws Exception {
    File file = new File(File.createTempFile("proxyTestConverter", null).getPath() + ".queue");
    file.deleteOnExit();
    ProxyConfig config = new ProxyConfig();
    config.parseArguments(new String[]{
        "--flushThreads", "2",
        "--buffer", file.getAbsolutePath(),
        "--exportQueuePorts", "2878",
        "--exportQueueOutputFile", file.getAbsolutePath() + "-output",
        "--exportQueueRetainData", "false"
    }, "proxy");
    TaskQueueFactory taskQueueFactory = new TaskQueueFactoryImpl(config.getBufferFile(), false);
    EntityPropertiesFactory entityPropertiesFactory = new EntityPropertiesFactoryImpl(config);
    QueueExporter qe = new QueueExporter(config, taskQueueFactory, entityPropertiesFactory);
    BufferedWriter mockedWriter = EasyMock.createMock(BufferedWriter.class);
    reset(mockedWriter);
    HandlerKey key = HandlerKey.of(ReportableEntityType.POINT, "2878");
    TaskQueue<LineDelimitedDataSubmissionTask> queue = taskQueueFactory.getTaskQueue(key, 0);
    queue.clear();
    UUID proxyId = UUID.randomUUID();
    LineDelimitedDataSubmissionTask task = new LineDelimitedDataSubmissionTask(null, proxyId,
        new DefaultEntityPropertiesForTesting(), queue, "wavefront", ReportableEntityType.POINT,
        "2878", ImmutableList.of("item1", "item2", "item3"), () -> 12345L);
    task.enqueue(QueueingReason.RETRY);
    LineDelimitedDataSubmissionTask task2 = new LineDelimitedDataSubmissionTask(null, proxyId,
        new DefaultEntityPropertiesForTesting(), queue, "wavefront", ReportableEntityType.POINT,
        "2878", ImmutableList.of("item4", "item5"), () -> 12345L);
    task2.enqueue(QueueingReason.RETRY);
    mockedWriter.write("item1");
    mockedWriter.newLine();
    mockedWriter.write("item2");
    mockedWriter.newLine();
    mockedWriter.write("item3");
    mockedWriter.newLine();
    mockedWriter.write("item4");
    mockedWriter.newLine();
    mockedWriter.write("item5");
    mockedWriter.newLine();

    TaskQueue<EventDataSubmissionTask> queue2 = taskQueueFactory.
        getTaskQueue(HandlerKey.of(ReportableEntityType.EVENT, "2888"), 0);
    queue2.clear();
    EventDataSubmissionTask eventTask = new EventDataSubmissionTask(null, proxyId,
        new DefaultEntityPropertiesForTesting(), queue2, "2888",
        ImmutableList.of(
            new Event(ReportEvent.newBuilder().
                setStartTime(123456789L * 1000).
                setEndTime(123456789L * 1000 + 1).
                setName("Event name for testing").
                setHosts(ImmutableList.of("host1", "host2")).
                setDimensions(ImmutableMap.of("multi", ImmutableList.of("bar", "baz"))).
                setAnnotations(ImmutableMap.of("severity", "INFO")).
                setTags(ImmutableList.of("tag1")).
                build()),
            new Event(ReportEvent.newBuilder().
                setStartTime(123456789L * 1000).
                setEndTime(123456789L * 1000 + 1).
                setName("Event name for testing").
                setHosts(ImmutableList.of("host1", "host2")).
                setAnnotations(ImmutableMap.of("severity", "INFO")).
                build()
            )),
        () -> 12345L);
    eventTask.enqueue(QueueingReason.RETRY);
    mockedWriter.write("@Event 123456789000 123456789001 \"Event name for testing\" " +
        "\"host\"=\"host1\" \"host\"=\"host2\" \"severity\"=\"INFO\" \"multi\"=\"bar\" " +
        "\"multi\"=\"baz\" \"tag\"=\"tag1\"");
    mockedWriter.newLine();
    mockedWriter.write("@Event 123456789000 123456789001 \"Event name for testing\" " +
        "\"host\"=\"host1\" \"host\"=\"host2\" \"severity\"=\"INFO\"");
    mockedWriter.newLine();

    TaskQueue<SourceTagSubmissionTask> queue3 = taskQueueFactory.
        getTaskQueue(HandlerKey.of(ReportableEntityType.SOURCE_TAG, "2898"), 0);
    queue3.clear();
    SourceTagSubmissionTask sourceTagTask = new SourceTagSubmissionTask(null,
        new DefaultEntityPropertiesForTesting(), queue3, "2898",
        new SourceTag(
            ReportSourceTag.newBuilder().setOperation(SourceOperationType.SOURCE_TAG).
                setAction(SourceTagAction.SAVE).setSource("testSource").
                setAnnotations(ImmutableList.of("newtag1", "newtag2")).build()),
        () -> 12345L);
    sourceTagTask.enqueue(QueueingReason.RETRY);
    mockedWriter.write("@SourceTag action=save source=\"testSource\" \"newtag1\" \"newtag2\"");
    mockedWriter.newLine();

    expectLastCall().once();
    replay(mockedWriter);

    assertEquals(2, queue.size());
    qe.processQueue(queue, mockedWriter);
    assertEquals(0, queue.size());

    assertEquals(1, queue2.size());
    qe.processQueue(queue2, mockedWriter);
    assertEquals(0, queue2.size());

    assertEquals(1, queue3.size());
    qe.processQueue(queue3, mockedWriter);
    assertEquals(0, queue3.size());

    verify(mockedWriter);

    List<String> files = QueueExporter.listFiles(file.getAbsolutePath()).stream().
        map(x -> x.replace(file.getAbsolutePath() + ".", "")).collect(Collectors.toList());
    assertEquals(3, files.size());
    assertTrue(files.contains("points.2878.0.spool"));
    assertTrue(files.contains("events.2888.0.spool"));
    assertTrue(files.contains("sourceTags.2898.0.spool"));

    HandlerKey k1 = HandlerKey.of(ReportableEntityType.POINT, "2878");
    HandlerKey k2 = HandlerKey.of(ReportableEntityType.EVENT, "2888");
    HandlerKey k3 = HandlerKey.of(ReportableEntityType.SOURCE_TAG, "2898");
    files = QueueExporter.listFiles(file.getAbsolutePath());
    Set<HandlerKey> hk = QueueExporter.getValidHandlerKeys(files, "all");
    assertEquals(3, hk.size());
    assertTrue(hk.contains(k1));
    assertTrue(hk.contains(k2));
    assertTrue(hk.contains(k3));

    hk = QueueExporter.getValidHandlerKeys(files, "2878, 2898");
    assertEquals(2, hk.size());
    assertTrue(hk.contains(k1));
    assertTrue(hk.contains(k3));

    hk = QueueExporter.getValidHandlerKeys(files, "2888");
    assertEquals(1, hk.size());
    assertTrue(hk.contains(k2));
  }

  @Test
  public void testQueueExporterWithRetainData() throws Exception {
    File file = new File(File.createTempFile("proxyTestConverter", null).getPath() + ".queue");
    file.deleteOnExit();
    ProxyConfig config = new ProxyConfig();
    config.parseArguments(new String[]{
        "--flushThreads", "2",
        "--buffer", file.getAbsolutePath(),
        "--exportQueuePorts", "2878",
        "--exportQueueOutputFile", file.getAbsolutePath() + "-output",
        "--exportQueueRetainData", "true"
    }, "proxy");
    TaskQueueFactory taskQueueFactory = new TaskQueueFactoryImpl(config.getBufferFile(), false);
    EntityPropertiesFactory entityPropertiesFactory = new EntityPropertiesFactoryImpl(config);
    QueueExporter qe = new QueueExporter(config, taskQueueFactory, entityPropertiesFactory);
    BufferedWriter mockedWriter = EasyMock.createMock(BufferedWriter.class);
    reset(mockedWriter);
    HandlerKey key = HandlerKey.of(ReportableEntityType.POINT, "2878");
    TaskQueue<LineDelimitedDataSubmissionTask> queue = taskQueueFactory.getTaskQueue(key, 0);
    queue.clear();
    UUID proxyId = UUID.randomUUID();
    LineDelimitedDataSubmissionTask task = new LineDelimitedDataSubmissionTask(null, proxyId,
        new DefaultEntityPropertiesForTesting(), queue, "wavefront", ReportableEntityType.POINT,
        "2878", ImmutableList.of("item1", "item2", "item3"), () -> 12345L);
    task.enqueue(QueueingReason.RETRY);
    LineDelimitedDataSubmissionTask task2 = new LineDelimitedDataSubmissionTask(null, proxyId,
        new DefaultEntityPropertiesForTesting(), queue, "wavefront", ReportableEntityType.POINT,
        "2878", ImmutableList.of("item4", "item5"), () -> 12345L);
    task2.enqueue(QueueingReason.RETRY);
    queue.close();

    qe.export();
    File outputTextFile = new File(file.getAbsolutePath() + "-output.points.2878.0.txt");
    assertEquals(ImmutableList.of("item1", "item2", "item3", "item4", "item5"),
        Files.asCharSource(outputTextFile, Charsets.UTF_8).readLines());
    assertEquals(2, taskQueueFactory.getTaskQueue(key, 0).size());
  }
}
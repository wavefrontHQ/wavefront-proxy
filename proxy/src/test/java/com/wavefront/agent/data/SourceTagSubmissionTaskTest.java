package com.wavefront.agent.data;

import static com.wavefront.agent.ProxyContext.queuesManager;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.senders.SenderStats;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.SourceTag;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import javax.ws.rs.core.Response;
import org.easymock.EasyMock;
import org.junit.Ignore;
import org.junit.Test;
import wavefront.report.ReportSourceTag;
import wavefront.report.SourceOperationType;
import wavefront.report.SourceTagAction;

/** @author vasily@wavefront.com */
@Ignore // i don't see the need for this
public class SourceTagSubmissionTaskTest {

  private final EntityProperties props = new DefaultEntityPropertiesForTesting();
  private SourceTagAPI sourceTagAPI = EasyMock.createMock(SourceTagAPI.class);

  @Test
  public void test200() {
    ReportSourceTag sourceDescDelete =
        new ReportSourceTag(
            SourceOperationType.SOURCE_DESCRIPTION,
            SourceTagAction.DELETE,
            "dummy",
            ImmutableList.of());
    ReportSourceTag sourceTagDelete =
        new ReportSourceTag(
            SourceOperationType.SOURCE_TAG, SourceTagAction.DELETE, "src", ImmutableList.of("tag"));
    ReportSourceTag sourceTagAdd =
        new ReportSourceTag(
            SourceOperationType.SOURCE_TAG, SourceTagAction.ADD, "src", ImmutableList.of("tag"));
    QueueInfo queue = queuesManager.initQueue(ReportableEntityType.SOURCE_TAG);
    SenderStats stats = SenderStats.create(queue, new ScheduledThreadPoolExecutor(0));
    SourceTagSubmissionTask task =
        new SourceTagSubmissionTask(
            sourceTagAPI,
            props,
            queue,
            new SourceTag(sourceDescDelete),
            System::currentTimeMillis,
            stats);
    SourceTagSubmissionTask task2 =
        new SourceTagSubmissionTask(
            sourceTagAPI,
            props,
            queue,
            new SourceTag(sourceTagDelete),
            System::currentTimeMillis,
            stats);
    SourceTagSubmissionTask task3 =
        new SourceTagSubmissionTask(
            sourceTagAPI,
            props,
            queue,
            new SourceTag(sourceTagAdd),
            System::currentTimeMillis,
            stats);
    expect(sourceTagAPI.removeDescription("dummy")).andReturn(Response.status(200).build()).once();
    expect(sourceTagAPI.removeTag("src", "tag")).andReturn(Response.status(200).build()).once();
    expect(sourceTagAPI.appendTag("src", "tag")).andReturn(Response.status(200).build()).once();
    assertEquals(TaskResult.DELIVERED, task.execute());
    assertEquals(TaskResult.DELIVERED, task2.execute());
    assertEquals(TaskResult.DELIVERED, task3.execute());
  }

  @Test
  public void test404() throws Exception {
    ReportSourceTag sourceDescDelete =
        new ReportSourceTag(
            SourceOperationType.SOURCE_DESCRIPTION,
            SourceTagAction.DELETE,
            "dummy",
            ImmutableList.of());
    ReportSourceTag sourceTagDelete =
        new ReportSourceTag(
            SourceOperationType.SOURCE_TAG, SourceTagAction.DELETE, "src", ImmutableList.of("tag"));
    ReportSourceTag sourceTagAdd =
        new ReportSourceTag(
            SourceOperationType.SOURCE_TAG, SourceTagAction.ADD, "src", ImmutableList.of("tag"));
    QueueInfo queue = queuesManager.initQueue(ReportableEntityType.SOURCE_TAG);
    SenderStats stats = SenderStats.create(queue, new ScheduledThreadPoolExecutor(0));
    SourceTagSubmissionTask task =
        new SourceTagSubmissionTask(
            sourceTagAPI,
            props,
            queue,
            new SourceTag(sourceDescDelete),
            System::currentTimeMillis,
            stats);
    SourceTagSubmissionTask task2 =
        new SourceTagSubmissionTask(
            sourceTagAPI,
            props,
            queue,
            new SourceTag(sourceTagDelete),
            System::currentTimeMillis,
            stats);
    SourceTagSubmissionTask task3 =
        new SourceTagSubmissionTask(
            sourceTagAPI,
            props,
            queue,
            new SourceTag(sourceTagAdd),
            System::currentTimeMillis,
            stats);
    expect(sourceTagAPI.removeDescription("dummy")).andReturn(Response.status(404).build()).once();
    expect(sourceTagAPI.removeTag("src", "tag")).andReturn(Response.status(404).build()).once();
    expect(sourceTagAPI.appendTag("src", "tag")).andReturn(Response.status(404).build()).once();
    expectLastCall();

    assertEquals(TaskResult.DELIVERED, task.execute());
    assertEquals(TaskResult.DELIVERED, task2.execute());
    assertEquals(TaskResult.PERSISTED, task3.execute());
  }

  @Test
  public void test500() throws Exception {
    ReportSourceTag sourceDescDelete =
        new ReportSourceTag(
            SourceOperationType.SOURCE_DESCRIPTION,
            SourceTagAction.DELETE,
            "dummy",
            ImmutableList.of());
    ReportSourceTag sourceTagDelete =
        new ReportSourceTag(
            SourceOperationType.SOURCE_TAG, SourceTagAction.DELETE, "src", ImmutableList.of("tag"));
    ReportSourceTag sourceTagAdd =
        new ReportSourceTag(
            SourceOperationType.SOURCE_TAG, SourceTagAction.ADD, "src", ImmutableList.of("tag"));
    QueueInfo queue = queuesManager.initQueue(ReportableEntityType.SOURCE_TAG);
    SenderStats stats = SenderStats.create(queue, new ScheduledThreadPoolExecutor(0));
    SourceTagSubmissionTask task =
        new SourceTagSubmissionTask(
            sourceTagAPI,
            props,
            queue,
            new SourceTag(sourceDescDelete),
            System::currentTimeMillis,
            stats);
    SourceTagSubmissionTask task2 =
        new SourceTagSubmissionTask(
            sourceTagAPI,
            props,
            queue,
            new SourceTag(sourceTagDelete),
            System::currentTimeMillis,
            stats);
    SourceTagSubmissionTask task3 =
        new SourceTagSubmissionTask(
            sourceTagAPI,
            props,
            queue,
            new SourceTag(sourceTagAdd),
            System::currentTimeMillis,
            stats);
    expect(sourceTagAPI.removeDescription("dummy")).andReturn(Response.status(500).build()).once();
    expect(sourceTagAPI.removeTag("src", "tag")).andReturn(Response.status(500).build()).once();
    expect(sourceTagAPI.appendTag("src", "tag")).andReturn(Response.status(500).build()).once();
    expectLastCall();
    assertEquals(TaskResult.PERSISTED, task.execute());
    assertEquals(TaskResult.PERSISTED, task2.execute());
    assertEquals(TaskResult.PERSISTED, task3.execute());
  }
}

package com.wavefront.agent.data;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.SourceTag;
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

  private QueueInfo queue = EasyMock.createMock(QueueInfo.class);
  private Response mockResponse = EasyMock.createMock(Response.class);

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
    expect(queue.getName()).andReturn("").anyTimes();
    expect(queue.getEntityType()).andReturn(ReportableEntityType.SOURCE_TAG).anyTimes();
    replay(queue);
    QueueStats stats = QueueStats.get(queue.getName());
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
    expect(mockResponse.getStatus()).andReturn(200).times(3);
    expect(sourceTagAPI.removeDescription("dummy")).andReturn(Response.status(200).build()).once();
    expect(sourceTagAPI.removeTag("src", "tag")).andReturn(Response.status(200).build()).once();
    expect(sourceTagAPI.appendTag("src", "tag")).andReturn(Response.status(200).build()).once();
    replay(sourceTagAPI, mockResponse);
    // Note: Changed TaskResult.DELIVERED to 0 as AbstractDataSubmissionTask execute() has
    // changed return type from TaskResult to int
    assertEquals(0, task.execute());
    assertEquals(0, task2.execute());
    assertEquals(0, task3.execute());
  }

  @Test(expected = RuntimeException.class)
  public void test404_RemoveDescription() {
    ReportSourceTag sourceDescDelete =
        new ReportSourceTag(
            SourceOperationType.SOURCE_DESCRIPTION,
            SourceTagAction.DELETE,
            "dummy",
            ImmutableList.of());
    expect(queue.getName()).andReturn("").anyTimes();
    expect(queue.getEntityType()).andReturn(ReportableEntityType.SOURCE_TAG).anyTimes();
    replay(queue);
    QueueStats stats = QueueStats.get(queue.getName());
    SourceTagSubmissionTask task =
        new SourceTagSubmissionTask(
            sourceTagAPI,
            props,
            queue,
            new SourceTag(sourceDescDelete),
            System::currentTimeMillis,
            stats);
    expect(mockResponse.getStatus()).andReturn(404).once();
    expect(sourceTagAPI.removeDescription("dummy")).andReturn(Response.status(404).build()).once();
    expectLastCall();
    replay(sourceTagAPI, mockResponse);

    // Currently, status 404 returns RuntimeException("Unhandled DataSubmissionException", ex)
    task.execute();
  }

  @Test(expected = RuntimeException.class)
  public void test404_RemoveTag() {
    ReportSourceTag sourceTagDelete =
        new ReportSourceTag(
            SourceOperationType.SOURCE_TAG, SourceTagAction.DELETE, "src", ImmutableList.of("tag"));
    expect(queue.getName()).andReturn("").anyTimes();
    expect(queue.getEntityType()).andReturn(ReportableEntityType.SOURCE_TAG).anyTimes();
    replay(queue);
    QueueStats stats = QueueStats.get(queue.getName());
    SourceTagSubmissionTask task2 =
        new SourceTagSubmissionTask(
            sourceTagAPI,
            props,
            queue,
            new SourceTag(sourceTagDelete),
            System::currentTimeMillis,
            stats);
    expect(mockResponse.getStatus()).andReturn(404).once();
    expect(sourceTagAPI.removeTag("src", "tag")).andReturn(Response.status(404).build()).once();
    expectLastCall();
    replay(sourceTagAPI, mockResponse);

    // Currently, status 404 returns RuntimeException("Unhandled DataSubmissionException", ex)
    task2.execute();
  }

  @Test(expected = RuntimeException.class)
  public void test404_AddTag() {
    ReportSourceTag sourceTagAdd =
        new ReportSourceTag(
            SourceOperationType.SOURCE_TAG, SourceTagAction.ADD, "src", ImmutableList.of("tag"));
    expect(queue.getName()).andReturn("").anyTimes();
    expect(queue.getEntityType()).andReturn(ReportableEntityType.SOURCE_TAG).anyTimes();
    replay(queue);
    QueueStats stats = QueueStats.get(queue.getName());
    SourceTagSubmissionTask task3 =
        new SourceTagSubmissionTask(
            sourceTagAPI,
            props,
            queue,
            new SourceTag(sourceTagAdd),
            System::currentTimeMillis,
            stats);
    expect(mockResponse.getStatus()).andReturn(404).once();
    expect(sourceTagAPI.appendTag("src", "tag")).andReturn(Response.status(404).build()).once();
    expectLastCall();
    replay(sourceTagAPI, mockResponse);

    task3.execute();
  }

  @Test
  public void test500() {
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
    expect(queue.getName()).andReturn("").anyTimes();
    expect(queue.getEntityType()).andReturn(ReportableEntityType.SOURCE_TAG).anyTimes();
    replay(queue);
    QueueStats stats = QueueStats.get(queue.getName());
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
    expect(mockResponse.getStatus()).andReturn(500).once();
    expect(sourceTagAPI.removeDescription("dummy")).andReturn(Response.status(500).build()).once();
    expect(sourceTagAPI.removeTag("src", "tag")).andReturn(Response.status(500).build()).once();
    expect(sourceTagAPI.appendTag("src", "tag")).andReturn(Response.status(500).build()).once();
    expectLastCall();
    replay(sourceTagAPI, mockResponse);

    // Right now we are not throwing IgnoreStatusCodeException for 500
    assertEquals(500, task.execute());
    assertEquals(500, task2.execute());
    assertEquals(500, task3.execute());
  }
}

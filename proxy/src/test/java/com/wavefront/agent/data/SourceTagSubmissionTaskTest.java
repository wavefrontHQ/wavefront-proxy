package com.wavefront.agent.data;

import javax.ws.rs.core.Response;

import org.easymock.EasyMock;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.wavefront.agent.handlers.SenderTaskFactoryImpl;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.dto.SourceTag;

import wavefront.report.ReportSourceTag;
import wavefront.report.SourceOperationType;
import wavefront.report.SourceTagAction;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.*;

/**
 * @author vasily@wavefront.com
 */
public class SourceTagSubmissionTaskTest {

  private SourceTagAPI sourceTagAPI = EasyMock.createMock(SourceTagAPI.class);
  private final EntityProperties props = new DefaultEntityPropertiesForTesting();

  @Test
  public void test200() {
    TaskQueue<SourceTagSubmissionTask> queue = createMock(TaskQueue.class);
    reset(sourceTagAPI, queue);
    ReportSourceTag sourceDescDelete = new ReportSourceTag(SourceOperationType.SOURCE_DESCRIPTION,
        SourceTagAction.DELETE, "dummy", ImmutableList.of());
    ReportSourceTag sourceTagDelete = new ReportSourceTag(SourceOperationType.SOURCE_TAG,
        SourceTagAction.DELETE, "src", ImmutableList.of("tag"));
    ReportSourceTag sourceTagAdd = new ReportSourceTag(SourceOperationType.SOURCE_TAG,
        SourceTagAction.ADD, "src", ImmutableList.of("tag"));
    SourceTagSubmissionTask task = new SourceTagSubmissionTask(sourceTagAPI,
        props, queue, "2878", new SourceTag(sourceDescDelete), System::currentTimeMillis);
    SourceTagSubmissionTask task2 = new SourceTagSubmissionTask(sourceTagAPI,
        props, queue, "2878", new SourceTag(sourceTagDelete), System::currentTimeMillis);
    SourceTagSubmissionTask task3 = new SourceTagSubmissionTask(sourceTagAPI,
        props, queue, "2878", new SourceTag(sourceTagAdd), System::currentTimeMillis);
    expect(sourceTagAPI.removeDescription("dummy")).andReturn(Response.status(200).build()).once();
    expect(sourceTagAPI.removeTag("src", "tag")).andReturn(Response.status(200).build()).once();
    expect(sourceTagAPI.appendTag("src", "tag")).andReturn(Response.status(200).build()).once();
    replay(sourceTagAPI, queue);
    assertEquals(TaskResult.DELIVERED, task.execute());
    assertEquals(TaskResult.DELIVERED, task2.execute());
    assertEquals(TaskResult.DELIVERED, task3.execute());
    verify(sourceTagAPI, queue);
  }

  @Test
  public void test404() throws Exception {
    TaskQueue<SourceTagSubmissionTask> queue = createMock(TaskQueue.class);
    reset(sourceTagAPI, queue);
    ReportSourceTag sourceDescDelete = new ReportSourceTag(SourceOperationType.SOURCE_DESCRIPTION,
        SourceTagAction.DELETE, "dummy", ImmutableList.of());
    ReportSourceTag sourceTagDelete = new ReportSourceTag(SourceOperationType.SOURCE_TAG,
        SourceTagAction.DELETE, "src", ImmutableList.of("tag"));
    ReportSourceTag sourceTagAdd = new ReportSourceTag(SourceOperationType.SOURCE_TAG,
        SourceTagAction.ADD, "src", ImmutableList.of("tag"));
    SourceTagSubmissionTask task = new SourceTagSubmissionTask(sourceTagAPI,
        props, queue, "2878", new SourceTag(sourceDescDelete), System::currentTimeMillis);
    SourceTagSubmissionTask task2 = new SourceTagSubmissionTask(sourceTagAPI,
        props, queue, "2878", new SourceTag(sourceTagDelete), System::currentTimeMillis);
    SourceTagSubmissionTask task3 = new SourceTagSubmissionTask(sourceTagAPI,
        props, queue, "2878", new SourceTag(sourceTagAdd), System::currentTimeMillis);
    expect(sourceTagAPI.removeDescription("dummy")).andReturn(Response.status(404).build()).once();
    expect(sourceTagAPI.removeTag("src", "tag")).andReturn(Response.status(404).build()).once();
    expect(sourceTagAPI.appendTag("src", "tag")).andReturn(Response.status(404).build()).once();
    queue.add(task3);
    expectLastCall();
    replay(sourceTagAPI, queue);

    assertEquals(TaskResult.DELIVERED, task.execute());
    assertEquals(TaskResult.DELIVERED, task2.execute());
    assertEquals(TaskResult.PERSISTED, task3.execute());
    verify(sourceTagAPI, queue);
  }

  @Test
  public void test500() throws Exception {
    TaskQueue<SourceTagSubmissionTask> queue = createMock(TaskQueue.class);
    reset(sourceTagAPI, queue);
    ReportSourceTag sourceDescDelete = new ReportSourceTag(SourceOperationType.SOURCE_DESCRIPTION,
        SourceTagAction.DELETE, "dummy", ImmutableList.of());
    ReportSourceTag sourceTagDelete = new ReportSourceTag(SourceOperationType.SOURCE_TAG,
        SourceTagAction.DELETE, "src", ImmutableList.of("tag"));
    ReportSourceTag sourceTagAdd = new ReportSourceTag(SourceOperationType.SOURCE_TAG,
        SourceTagAction.ADD, "src", ImmutableList.of("tag"));
    SourceTagSubmissionTask task = new SourceTagSubmissionTask(sourceTagAPI,
        props, queue, "2878", new SourceTag(sourceDescDelete), System::currentTimeMillis);
    SourceTagSubmissionTask task2 = new SourceTagSubmissionTask(sourceTagAPI,
        props, queue, "2878", new SourceTag(sourceTagDelete), System::currentTimeMillis);
    SourceTagSubmissionTask task3 = new SourceTagSubmissionTask(sourceTagAPI,
        props, queue, "2878", new SourceTag(sourceTagAdd), System::currentTimeMillis);
    expect(sourceTagAPI.removeDescription("dummy")).andReturn(Response.status(500).build()).once();
    expect(sourceTagAPI.removeTag("src", "tag")).andReturn(Response.status(500).build()).once();
    expect(sourceTagAPI.appendTag("src", "tag")).andReturn(Response.status(500).build()).once();
    queue.add(task);
    queue.add(task2);
    queue.add(task3);
    expectLastCall();
    replay(sourceTagAPI, queue);
    assertEquals(TaskResult.PERSISTED, task.execute());
    assertEquals(TaskResult.PERSISTED, task2.execute());
    assertEquals(TaskResult.PERSISTED, task3.execute());
    verify(sourceTagAPI, queue);
  }
}

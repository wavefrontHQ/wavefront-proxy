package com.wavefront.agent.handlers;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RecyclableRateLimiter;

import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;
import com.wavefront.data.ReportableEntityType;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.core.Response;

import wavefront.report.ReportSourceTag;

/**
 * This class tests the ReportSourceTagHandler.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com)
 */
public class ReportSourceTagHandlerTest {

  private ReportSourceTagHandlerImpl sourceTagHandler;
  private SenderTaskFactory senderTaskFactory;
  private ForceQueueEnabledAgentAPI mockAgentAPI;
  private UUID newAgentId;

  @Before
  public void setup() {
    mockAgentAPI = EasyMock.createMock(ForceQueueEnabledAgentAPI.class);
    newAgentId = UUID.randomUUID();
    senderTaskFactory = new SenderTaskFactoryImpl(mockAgentAPI, newAgentId, null, new AtomicInteger(100),
        new AtomicInteger(10), new AtomicInteger(1000));
    sourceTagHandler = new ReportSourceTagHandlerImpl("4878", 10, senderTaskFactory.createSenderTasks(
        HandlerKey.of(ReportableEntityType.SOURCE_TAG, "4878"), 2));
  }

  /**
   * This test will add 3 source tags and verify that the server side api is called properly.
   *
   */
  @Test
  public void testSourceTagsSetting() throws Exception {
    String[] annotations = new String[]{"tag1", "tag2", "tag3"};
    ReportSourceTag sourceTag = new ReportSourceTag("SourceTag", "save", "dummy", "desc", Arrays.asList
        (annotations));
    EasyMock.expect(mockAgentAPI.setTags("dummy", Arrays.asList(annotations), false)).andReturn(
        Response.ok().build()).once();

    EasyMock.replay(mockAgentAPI);

    sourceTagHandler.report(sourceTag);
    TimeUnit.SECONDS.sleep(1);
    EasyMock.verify(mockAgentAPI);
  }

  @Test
  public void testSourceTagsTaskAffinity() {
    ReportSourceTag sourceTag1 = new ReportSourceTag("SourceTag", "save", "dummy", "desc",
        ImmutableList.of("tag1", "tag2"));
    ReportSourceTag sourceTag2 = new ReportSourceTag("SourceTag", "save", "dummy", "desc 2",
        ImmutableList.of("tag2", "tag3"));
    ReportSourceTag sourceTag3 = new ReportSourceTag("SourceTag", "save", "dummy-2", "desc 3",
        ImmutableList.of("tag3"));
    ReportSourceTag sourceTag4 = new ReportSourceTag("SourceTag", "save", "dummy", "desc 4",
        ImmutableList.of("tag1", "tag4", "tag5"));
    List<SenderTask> tasks = new ArrayList<>();
    ReportSourceTagSenderTask task1 = EasyMock.createMock(ReportSourceTagSenderTask.class);
    ReportSourceTagSenderTask task2 = EasyMock.createMock(ReportSourceTagSenderTask.class);
    tasks.add(task1);
    tasks.add(task2);
    ReportSourceTagHandlerImpl sourceTagHandler = new ReportSourceTagHandlerImpl("4878", 10, tasks);
    task1.add(sourceTag1);
    EasyMock.expectLastCall();
    task1.add(sourceTag2);
    EasyMock.expectLastCall();
    task2.add(sourceTag3);
    EasyMock.expectLastCall();
    task1.add(sourceTag4);
    EasyMock.expectLastCall();
    task1.add(sourceTag4);
    EasyMock.expectLastCall();
    task2.add(sourceTag3);
    EasyMock.expectLastCall();
    task1.add(sourceTag2);
    EasyMock.expectLastCall();
    task1.add(sourceTag1);
    EasyMock.expectLastCall();

    EasyMock.replay(task1);
    EasyMock.replay(task2);

    sourceTagHandler.report(sourceTag1);
    sourceTagHandler.report(sourceTag2);
    sourceTagHandler.report(sourceTag3);
    sourceTagHandler.report(sourceTag4);
    sourceTagHandler.report(sourceTag4);
    sourceTagHandler.report(sourceTag3);
    sourceTagHandler.report(sourceTag2);
    sourceTagHandler.report(sourceTag1);

    EasyMock.verify();
  }
}

package com.wavefront.agent.handlers;

import com.google.common.collect.ImmutableList;

import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.data.DefaultEntityPropertiesForTesting;
import com.wavefront.agent.data.EntityPropertiesFactory;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.agent.queueing.TaskQueueFactory;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.data.ReportableEntityType;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
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
  private SourceTagAPI mockAgentAPI;
  private TaskQueueFactory taskQueueFactory;
  private UUID newAgentId;
  private Logger blockedLogger = Logger.getLogger("RawBlockedPoints");

  @Before
  public void setup() {
    mockAgentAPI = EasyMock.createMock(SourceTagAPI.class);
    taskQueueFactory = new TaskQueueFactory() {
      @Override
      public <T extends DataSubmissionTask<T>> TaskQueue<T> getTaskQueue(
          @Nonnull HandlerKey handlerKey, int threadNum) {
        return null;
      }
    };
    newAgentId = UUID.randomUUID();
    senderTaskFactory = new SenderTaskFactoryImpl(new APIContainer(null, mockAgentAPI, null),
        newAgentId, taskQueueFactory, null, type -> new DefaultEntityPropertiesForTesting());
    HandlerKey handlerKey = HandlerKey.of(ReportableEntityType.SOURCE_TAG, "4878");
    sourceTagHandler = new ReportSourceTagHandlerImpl(handlerKey, 10,
            senderTaskFactory.createSenderTasks(handlerKey, 2), blockedLogger);
  }

  /**
   * This test will add 3 source tags and verify that the server side api is called properly.
   *
   */
  @Test
  public void testSourceTagsSetting() throws Exception {
    String[] annotations = new String[]{"tag1", "tag2", "tag3"};
    ReportSourceTag sourceTag = new ReportSourceTag("SourceTag", "save", "dummy", "desc",
        Arrays.asList(annotations));
    EasyMock.expect(mockAgentAPI.setTags("dummy", Arrays.asList(annotations))).andReturn(
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
    List<SenderTask<ReportSourceTag>> tasks = new ArrayList<>();
    ReportSourceTagSenderTask task1 = EasyMock.createMock(ReportSourceTagSenderTask.class);
    ReportSourceTagSenderTask task2 = EasyMock.createMock(ReportSourceTagSenderTask.class);
    tasks.add(task1);
    tasks.add(task2);
    ReportSourceTagHandlerImpl sourceTagHandler = new ReportSourceTagHandlerImpl(
        HandlerKey.of(ReportableEntityType.SOURCE_TAG, "4878"), 10, tasks, blockedLogger);
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

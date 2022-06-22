package com.wavefront.agent.handlers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.buffer.BuffersManager;
import com.wavefront.agent.buffer.BuffersManagerConfig;
import com.wavefront.agent.data.DefaultEntityPropertiesFactoryForTesting;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.data.ReportableEntityType;
import edu.emory.mathcs.backport.java.util.Collections;
import java.util.*;
import java.util.logging.Logger;
import javax.ws.rs.core.Response;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import wavefront.report.ReportSourceTag;
import wavefront.report.SourceOperationType;
import wavefront.report.SourceTagAction;

/**
 * This class tests the ReportSourceTagHandler.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com)
 */
public class ReportSourceTagHandlerTest {

  private ReportSourceTagHandlerImpl sourceTagHandler;
  private SenderTaskFactory senderTaskFactory;
  private SourceTagAPI mockAgentAPI;
  private UUID newAgentId;
  private HandlerKey handlerKey;
  private Logger blockedLogger = Logger.getLogger("RawBlockedPoints");

  @Before
  public void setup() {
    mockAgentAPI = EasyMock.createMock(SourceTagAPI.class);
    newAgentId = UUID.randomUUID();
    senderTaskFactory =
        new SenderTaskFactoryImpl(
            new APIContainer(null, mockAgentAPI, null, null),
            newAgentId,
            Collections.singletonMap(
                APIContainer.CENTRAL_TENANT_NAME, new DefaultEntityPropertiesFactoryForTesting()));

    handlerKey = new HandlerKey(ReportableEntityType.SOURCE_TAG, "4878");
    sourceTagHandler = new ReportSourceTagHandlerImpl(handlerKey, 10, null, blockedLogger);

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.l2 = false;
    BuffersManager.init(cfg, senderTaskFactory);
    BuffersManager.registerNewQueueIfNeedIt(handlerKey);
  }

  /** This test will add 3 source tags and verify that the server side api is called properly. */
  @Test
  public void testSourceTagsSetting() {
    String[] annotations = new String[] {"tag1", "tag2", "tag3"};
    ReportSourceTag sourceTag =
        new ReportSourceTag(
            SourceOperationType.SOURCE_TAG,
            SourceTagAction.SAVE,
            "dummy",
            Arrays.asList(annotations));
    EasyMock.expect(mockAgentAPI.setTags("dummy", Arrays.asList(annotations)))
        .andReturn(Response.ok().build())
        .once();
    EasyMock.replay(mockAgentAPI);
    sourceTagHandler.report(sourceTag);
    EasyMock.verify(mockAgentAPI);
  }

  @Test
  public void testSourceTagAppend() {
    ReportSourceTag sourceTag =
        new ReportSourceTag(
            SourceOperationType.SOURCE_TAG, SourceTagAction.ADD, "dummy", ImmutableList.of("tag1"));
    EasyMock.expect(mockAgentAPI.appendTag("dummy", "tag1"))
        .andReturn(Response.ok().build())
        .once();
    EasyMock.replay(mockAgentAPI);
    sourceTagHandler.report(sourceTag);
    EasyMock.verify(mockAgentAPI);
  }

  @Test
  public void testSourceTagDelete() {
    ReportSourceTag sourceTag =
        new ReportSourceTag(
            SourceOperationType.SOURCE_TAG,
            SourceTagAction.DELETE,
            "dummy",
            ImmutableList.of("tag1"));
    EasyMock.expect(mockAgentAPI.removeTag("dummy", "tag1"))
        .andReturn(Response.ok().build())
        .once();
    EasyMock.replay(mockAgentAPI);
    sourceTagHandler.report(sourceTag);
    EasyMock.verify(mockAgentAPI);
  }

  @Test
  public void testSourceAddDescription() {
    ReportSourceTag sourceTag =
        new ReportSourceTag(
            SourceOperationType.SOURCE_DESCRIPTION,
            SourceTagAction.SAVE,
            "dummy",
            ImmutableList.of("description"));
    EasyMock.expect(mockAgentAPI.setDescription("dummy", "description"))
        .andReturn(Response.ok().build())
        .once();
    EasyMock.replay(mockAgentAPI);
    sourceTagHandler.report(sourceTag);
    EasyMock.verify(mockAgentAPI);
  }

  @Test
  public void testSourceDeleteDescription() {
    ReportSourceTag sourceTag =
        new ReportSourceTag(
            SourceOperationType.SOURCE_DESCRIPTION,
            SourceTagAction.DELETE,
            "dummy",
            ImmutableList.of());
    EasyMock.expect(mockAgentAPI.removeDescription("dummy"))
        .andReturn(Response.ok().build())
        .once();
    EasyMock.replay(mockAgentAPI);
    sourceTagHandler.report(sourceTag);
    EasyMock.verify(mockAgentAPI);
  }

  @Test
  public void testSourceTagsTaskAffinity() {
    ReportSourceTag sourceTag1 =
        new ReportSourceTag(
            SourceOperationType.SOURCE_TAG,
            SourceTagAction.SAVE,
            "dummy",
            ImmutableList.of("tag1", "tag2"));
    ReportSourceTag sourceTag2 =
        new ReportSourceTag(
            SourceOperationType.SOURCE_TAG,
            SourceTagAction.SAVE,
            "dummy",
            ImmutableList.of("tag2", "tag3"));
    ReportSourceTag sourceTag3 =
        new ReportSourceTag(
            SourceOperationType.SOURCE_TAG,
            SourceTagAction.SAVE,
            "dummy-2",
            ImmutableList.of("tag3"));
    ReportSourceTag sourceTag4 =
        new ReportSourceTag(
            SourceOperationType.SOURCE_TAG,
            SourceTagAction.SAVE,
            "dummy",
            ImmutableList.of("tag1", "tag4", "tag5"));
    List<SenderTask> tasks = new ArrayList<>();
    SourceTagSenderTask task1 = EasyMock.createMock(SourceTagSenderTask.class);
    SourceTagSenderTask task2 = EasyMock.createMock(SourceTagSenderTask.class);
    tasks.add(task1);
    tasks.add(task2);
    Map<String, Collection<SenderTask>> taskMap =
        ImmutableMap.of(APIContainer.CENTRAL_TENANT_NAME, tasks);
    ReportSourceTagHandlerImpl sourceTagHandler =
        new ReportSourceTagHandlerImpl(
            new HandlerKey(ReportableEntityType.SOURCE_TAG, "4878"), 10, null, blockedLogger);
    // todo: review
    //    task1.add(new SourceTag(sourceTag1));
    //    EasyMock.expectLastCall();
    //    task1.add(new SourceTag(sourceTag2));
    //    EasyMock.expectLastCall();
    //    task2.add(new SourceTag(sourceTag3));
    //    EasyMock.expectLastCall();
    //    task1.add(new SourceTag(sourceTag4));
    //    EasyMock.expectLastCall();
    //    task1.add(new SourceTag(sourceTag4));
    //    EasyMock.expectLastCall();
    //    task2.add(new SourceTag(sourceTag3));
    //    EasyMock.expectLastCall();
    //    task1.add(new SourceTag(sourceTag2));
    //    EasyMock.expectLastCall();
    //    task1.add(new SourceTag(sourceTag1));
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

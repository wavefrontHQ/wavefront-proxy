package com.wavefront.agent.core.handlers;

import static com.wavefront.agent.ProxyContext.queuesManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wavefront.agent.ProxyContext;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.buffers.BuffersManagerConfig;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.senders.SenderTask;
import com.wavefront.agent.core.senders.SenderTasksManager;
import com.wavefront.agent.core.senders.SourceTagSenderTask;
import com.wavefront.agent.data.DefaultEntityPropertiesFactoryForTesting;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.data.ReportableEntityType;
import edu.emory.mathcs.backport.java.util.Collections;
import java.util.*;
import java.util.logging.Logger;
import javax.ws.rs.core.Response;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import wavefront.report.ReportSourceTag;
import wavefront.report.SourceOperationType;
import wavefront.report.SourceTagAction;

/**
 * This class tests the ReportSourceTagHandler.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com)
 */
@Ignore // already tested on "testEndToEndSourceTags"
public class ReportSourceTagHandlerTest {

  private ReportSourceTagHandlerImpl sourceTagHandler;
  private SourceTagAPI mockAgentAPI;
  private UUID newAgentId;
  private QueueInfo handlerKey;
  private Logger blockedLogger = Logger.getLogger("RawBlockedPoints");

  @Before
  public void setup() {
    mockAgentAPI = EasyMock.createMock(SourceTagAPI.class);
    newAgentId = UUID.randomUUID();
    ProxyContext.entityPropertiesFactoryMap =
        Collections.singletonMap(
            APIContainer.CENTRAL_TENANT_NAME, new DefaultEntityPropertiesFactoryForTesting());

    SenderTasksManager.init(new APIContainer(null, mockAgentAPI, null, null), newAgentId);

    handlerKey = queuesManager.initQueue(ReportableEntityType.SOURCE_TAG);
    sourceTagHandler = new ReportSourceTagHandlerImpl("4878", handlerKey, 10, blockedLogger);

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.disk = false;
    BuffersManager.init(cfg);
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
  public void testSourceTagDelete() throws InterruptedException {
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
            "4878", queuesManager.initQueue(ReportableEntityType.SOURCE_TAG), 10, blockedLogger);
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

package com.wavefront.agent.handlers;

import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;
import com.wavefront.data.ReportableEntityType;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
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
}

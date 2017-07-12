package com.wavefront.agent;

import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;

import org.apache.commons.lang3.StringUtils;
import org.easymock.EasyMock;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.ws.rs.core.Response;

import wavefront.report.ReportSourceTag;

/**
 * This class tests the SourceTagHandler.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com)
 */
public class SourceTagHandlerTest {

  private static SourceTagHandlerImpl sourceTagHandler;
  private static QueuedAgentService queuedAgentService;
  private static ForceQueueEnabledAgentAPI mockAgentAPI;
  private static UUID newAgentId;

  @BeforeClass
  public static void setup() throws IOException {
    mockAgentAPI = EasyMock.createMock(ForceQueueEnabledAgentAPI.class);
    newAgentId = UUID.randomUUID();
    int retryThreads = 1;
    queuedAgentService = new QueuedAgentService(mockAgentAPI, "unitTestBuffer2", retryThreads,
        Executors.newScheduledThreadPool(retryThreads + 1, new ThreadFactory() {

          private AtomicLong counter = new AtomicLong();

          @Override
          public Thread newThread(Runnable r) {
            Thread toReturn = new Thread(r);
            toReturn.setName("unit test submission worker: " + counter.getAndIncrement());
            return toReturn;
          }
        }), true, newAgentId, false, "NONE");
    sourceTagHandler = new SourceTagHandlerImpl(getSourceTagFlushTasks(4878));
  }

  /**
   * Create an executor to run the post sourceTag tasks.
   *
   * @param port
   * @return
   */
  private static PostSourceTagTimedTask[] getSourceTagFlushTasks(int port) {
    int flushThreads = 2;
    PostSourceTagTimedTask[] toReturn = new PostSourceTagTimedTask[flushThreads];
    for (int i = 0; i < flushThreads; i++) {
      final PostSourceTagTimedTask postSourceTagTimedTask = new PostSourceTagTimedTask(mockAgentAPI,
          "NONE", port, i, 100, StringUtils.EMPTY);
      toReturn[i] = postSourceTagTimedTask;
    }
    return toReturn;
  }

  /**
   * This test will add 3 source tags and verify that the server side api is called properly.
   *
   * @throws Exception
   */
  @Test
  public void testSourceTagsSetting() throws Exception {
    ReportSourceTag[] sourceTags = new ReportSourceTag[1];
    String[] annotations = new String[]{"tag1", "tag2", "tag3"};
    sourceTags[0] = new ReportSourceTag("SourceTag", "save", "dummy", "desc", Arrays.asList
        (annotations));
    EasyMock.expect(mockAgentAPI.setTags("dummy", StringUtils.EMPTY, Arrays.asList(annotations), false)).andReturn(
        Response.ok().build()).once();

    EasyMock.replay(mockAgentAPI);

    sourceTagHandler.reportSourceTags(Arrays.asList(sourceTags));
    TimeUnit.SECONDS.sleep(2);
    EasyMock.verify(mockAgentAPI);
  }
}

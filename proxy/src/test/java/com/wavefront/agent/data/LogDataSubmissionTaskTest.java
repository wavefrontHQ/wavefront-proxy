package com.wavefront.agent.data;

import static com.wavefront.agent.data.LogDataSubmissionTask.AGENT_PREFIX;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.LogAPI;
import com.wavefront.dto.Log;
import java.io.IOException;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.easymock.EasyMock;
import org.junit.Test;
import wavefront.report.ReportLog;

public class LogDataSubmissionTaskTest {

  private final LogAPI logAPI = EasyMock.createMock(LogAPI.class);
  private final EntityProperties props = new DefaultEntityPropertiesForTesting();

  @Test
  public void test429() throws IOException {
    TaskQueue<LogDataSubmissionTask> queue = createMock(TaskQueue.class);
    reset(logAPI, queue);
    ReportLog testLog = new ReportLog(0L, "msg", "host", ImmutableList.of());
    Log log = new Log(testLog);
    UUID uuid = UUID.randomUUID();
    LogDataSubmissionTask task =
        new LogDataSubmissionTask(
            logAPI, uuid, props, queue, "2878", ImmutableList.of(log), System::currentTimeMillis);
    expect(logAPI.proxyLogs(AGENT_PREFIX + uuid, ImmutableList.of(log)))
        .andReturn(Response.status(429).build())
        .once();
    expectLastCall();
    replay(logAPI, queue);
    assertEquals(TaskResult.REMOVED, task.execute());
    verify(logAPI, queue);
  }
}

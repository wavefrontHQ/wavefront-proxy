package com.wavefront.agent;

import com.squareup.tape.TaskInjector;
import com.wavefront.agent.QueuedAgentService.PostPushDataResultTask;
import com.wavefront.api.AgentAPI;
import com.wavefront.api.agent.ShellOutputDTO;
import net.jcip.annotations.NotThreadSafe;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Andrew Kao (andrew@wavefront.com)
 */
@NotThreadSafe
public class QueuedAgentServiceTest {

  private QueuedAgentService queuedAgentService;
  private AgentAPI mockAgentAPI;

  @Before
  public void testSetup() throws IOException {
    mockAgentAPI = EasyMock.createMock(AgentAPI.class);

    int retryThreads = 1;

    queuedAgentService = new QueuedAgentService(mockAgentAPI, "unitTestBuffer", retryThreads,
        Executors.newScheduledThreadPool(retryThreads + 1, new ThreadFactory() {

          private AtomicLong counter = new AtomicLong();

          @Override
          public Thread newThread(Runnable r) {
            Thread toReturn = new Thread(r);
            toReturn.setName("unit test submission worker: " + counter.getAndIncrement());
            return toReturn;
          }
        }), true);
  }

  // postWorkUnitResult

  @Test
  public void postWorkUnitResultCallsApproriateServiceMethodAndReturnsOK() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();
    UUID targetId = UUID.randomUUID();

    ShellOutputDTO shellOutputDTO = new ShellOutputDTO();

    EasyMock.expect(mockAgentAPI.postWorkUnitResult(agentId, workUnitId, targetId, shellOutputDTO)).
        andReturn(Response.ok().build()).once();
    EasyMock.replay(mockAgentAPI);

    Response response = queuedAgentService.postWorkUnitResult(agentId, workUnitId, targetId, shellOutputDTO);

    EasyMock.verify(mockAgentAPI);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void postWorkUnitResultServiceReturns406RequeuesAndReturnsNotAcceptable() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();
    UUID targetId = UUID.randomUUID();

    ShellOutputDTO shellOutputDTO = new ShellOutputDTO();

    EasyMock.expect(mockAgentAPI.postWorkUnitResult(agentId, workUnitId, targetId, shellOutputDTO)).
        andReturn(Response.status(Response.Status.NOT_ACCEPTABLE).build()).once();
    EasyMock.replay(mockAgentAPI);

    Response response = queuedAgentService.postWorkUnitResult(agentId, workUnitId, targetId, shellOutputDTO);

    EasyMock.verify(mockAgentAPI);
    assertEquals(Response.Status.NOT_ACCEPTABLE.getStatusCode(), response.getStatus());
    assertEquals(1, queuedAgentService.getQueuedTasksCount());
  }

  @Test
  public void postWorkUnitResultServiceReturns413RequeuesAndReturnsNotAcceptable() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();
    UUID targetId = UUID.randomUUID();

    ShellOutputDTO shellOutputDTO = new ShellOutputDTO();

    EasyMock.expect(mockAgentAPI.postWorkUnitResult(agentId, workUnitId, targetId, shellOutputDTO)).
        andReturn(Response.status(Response.Status.REQUEST_ENTITY_TOO_LARGE).build()).once();
    EasyMock.replay(mockAgentAPI);

    Response response = queuedAgentService.postWorkUnitResult(agentId, workUnitId, targetId, shellOutputDTO);

    EasyMock.verify(mockAgentAPI);
    assertEquals(Response.Status.NOT_ACCEPTABLE.getStatusCode(), response.getStatus());
    assertEquals(1, queuedAgentService.getQueuedTasksCount());
  }

  // postPushData
  @Test
  public void postPushDataCallsApproriateServiceMethodAndReturnsOK() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();

    long now = System.currentTimeMillis();

    String format = "unitTestFormat";

    List<String> pretendPushDataList = new ArrayList<String>();
    pretendPushDataList.add("string line 1");
    pretendPushDataList.add("string line 2");

    String pretendPushData = GraphiteStringHandler.joinPushData(pretendPushDataList);

    EasyMock.expect(mockAgentAPI.postPushData(agentId, workUnitId, now, format, pretendPushData)).
        andReturn(Response.ok().build()).once();
    EasyMock.replay(mockAgentAPI);

    Response response = queuedAgentService.postPushData(agentId, workUnitId, now, format, pretendPushData);

    EasyMock.verify(mockAgentAPI);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void postPushDataServiceReturns406RequeuesAndReturnsNotAcceptable() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();
    long now = System.currentTimeMillis();

    String format = "unitTestFormat";

    List<String> pretendPushDataList = new ArrayList<String>();
    pretendPushDataList.add("string line 1");
    pretendPushDataList.add("string line 2");

    String pretendPushData = GraphiteStringHandler.joinPushData(pretendPushDataList);

    EasyMock.expect(mockAgentAPI.postPushData(agentId, workUnitId, now, format, pretendPushData)).
        andReturn(Response.status(Response.Status.NOT_ACCEPTABLE).build()).once();
    EasyMock.replay(mockAgentAPI);

    Response response = queuedAgentService.postPushData(agentId, workUnitId, now, format, pretendPushData);

    EasyMock.verify(mockAgentAPI);
    assertEquals(Response.Status.NOT_ACCEPTABLE.getStatusCode(), response.getStatus());
    assertEquals(1, queuedAgentService.getQueuedTasksCount());
  }

  @Test
  public void postPushDataServiceReturns413RequeuesAndReturnsNotAcceptableAndSplitsDataAndSuccessfullySendsIt() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();
    long now = System.currentTimeMillis();

    String format = "unitTestFormat";

    List<String> pretendPushDataList = new ArrayList<String>();

    String str1 = "string line 1";
    String str2 = "string line 2";

    pretendPushDataList.add(str1);
    pretendPushDataList.add(str2);

    String pretendPushData = GraphiteStringHandler.joinPushData(pretendPushDataList);

    EasyMock.expect(mockAgentAPI.postPushData(agentId, workUnitId, now, format, pretendPushData)).
        andReturn(Response.status(Response.Status.REQUEST_ENTITY_TOO_LARGE).build()).once();
    EasyMock.expect(mockAgentAPI.postPushData(agentId, workUnitId, now, format, str1)).
        andReturn(Response.ok().build()).once();
    EasyMock.expect(mockAgentAPI.postPushData(agentId, workUnitId, now, format, str2)).
        andReturn(Response.ok().build()).once();

    EasyMock.replay(mockAgentAPI);

    Response response = queuedAgentService.postPushData(agentId, workUnitId, now, format, pretendPushData);

    EasyMock.verify(mockAgentAPI);
    assertEquals(Response.Status.NOT_ACCEPTABLE.getStatusCode(), response.getStatus());
    assertEquals(0, queuedAgentService.getQueuedTasksCount());
  }

  @Test
  public void postPushDataServiceReturns413RequeuesAndReturnsNotAcceptableAndSplitsDataAndQueuesIfFailsAgain() {
    // this is the same as the test above, but w/ the split still being too big
    // -- and instead of spinning on resends, this should actually add it to the Q
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();
    long now = System.currentTimeMillis();

    String format = "unitTestFormat";

    List<String> pretendPushDataList = new ArrayList<String>();

    String str1 = "string line 1";
    String str2 = "string line 2";

    pretendPushDataList.add(str1);
    pretendPushDataList.add(str2);

    String pretendPushData = GraphiteStringHandler.joinPushData(pretendPushDataList);

    EasyMock.expect(mockAgentAPI.postPushData(agentId, workUnitId, now, format, pretendPushData)).andReturn(Response
        .status(Response.Status.REQUEST_ENTITY_TOO_LARGE).build()).once();
    EasyMock.expect(mockAgentAPI.postPushData(agentId, workUnitId, now, format, str1)).andReturn(Response.status
        (Response.Status.REQUEST_ENTITY_TOO_LARGE).build()).once();
    EasyMock.expect(mockAgentAPI.postPushData(agentId, workUnitId, now, format, str2)).andReturn(Response.status
        (Response.Status.REQUEST_ENTITY_TOO_LARGE).build()).once();

    EasyMock.replay(mockAgentAPI);

    Response response = queuedAgentService.postPushData(agentId, workUnitId, now, format, pretendPushData);

    EasyMock.verify(mockAgentAPI);
    assertEquals(Response.Status.NOT_ACCEPTABLE.getStatusCode(), response.getStatus());
    assertEquals(2, queuedAgentService.getQueuedTasksCount());
  }

  private void injectServiceToResubmissionTask(ResubmissionTask task) {
    new TaskInjector<ResubmissionTask>() {
      @Override
      public void injectMembers(ResubmissionTask task) {
        task.service = mockAgentAPI;
      }
    }.injectMembers(task);
  }

  // *******
  // ** PostWorkUnitResultTask
  // *******
  @Test
  public void postWorkUnitResultTaskExecuteCallsAppropriateService() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();
    UUID targetId = UUID.randomUUID();

    ShellOutputDTO shellOutputDTO = new ShellOutputDTO();

    QueuedAgentService.PostWorkUnitResultTask task = new QueuedAgentService.PostWorkUnitResultTask(
        agentId,
        workUnitId,
        targetId,
        shellOutputDTO
    );

    injectServiceToResubmissionTask(task);

    EasyMock.expect(mockAgentAPI.postWorkUnitResult(agentId, workUnitId, targetId, shellOutputDTO))
        .andReturn(Response.ok().build()).once();

    EasyMock.replay(mockAgentAPI);

    task.execute(null);

    EasyMock.verify(mockAgentAPI);
  }

  @Test
  public void postWorkUnitResultTaskExecuteServiceReturns406ThrowsException() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();
    UUID targetId = UUID.randomUUID();

    ShellOutputDTO shellOutputDTO = new ShellOutputDTO();

    QueuedAgentService.PostWorkUnitResultTask task = new QueuedAgentService.PostWorkUnitResultTask(
        agentId,
        workUnitId,
        targetId,
        shellOutputDTO
    );

    injectServiceToResubmissionTask(task);

    EasyMock.expect(mockAgentAPI.postWorkUnitResult(agentId, workUnitId, targetId, shellOutputDTO))
        .andReturn(Response.status(Response.Status.NOT_ACCEPTABLE).build()).once();

    EasyMock.replay(mockAgentAPI);

    boolean exceptionThrown = false;

    try {
      task.execute(null);
    } catch (RejectedExecutionException e) {
      exceptionThrown = true;
    }

    Assert.assertTrue(exceptionThrown);
    EasyMock.verify(mockAgentAPI);
  }


  @Test
  public void postWorkUnitResultTaskExecuteServiceReturns413ThrowsException() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();
    UUID targetId = UUID.randomUUID();

    ShellOutputDTO shellOutputDTO = new ShellOutputDTO();

    QueuedAgentService.PostWorkUnitResultTask task = new QueuedAgentService.PostWorkUnitResultTask(
        agentId,
        workUnitId,
        targetId,
        shellOutputDTO
    );

    injectServiceToResubmissionTask(task);

    EasyMock.expect(mockAgentAPI.postWorkUnitResult(agentId, workUnitId, targetId, shellOutputDTO))
        .andReturn(Response.status(Response.Status.REQUEST_ENTITY_TOO_LARGE).build()).once();

    EasyMock.replay(mockAgentAPI);

    boolean exceptionThrown = false;

    try {
      task.execute(null);
    } catch (QueuedPushTooLargeException e) {
      exceptionThrown = true;
    }

    Assert.assertTrue(exceptionThrown);
    EasyMock.verify(mockAgentAPI);
  }

  @Test
  public void postWorkUnitResultTaskSplitReturnsListOfOne() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();
    UUID targetId = UUID.randomUUID();

    ShellOutputDTO shellOutputDTO = new ShellOutputDTO();

    QueuedAgentService.PostWorkUnitResultTask task = new QueuedAgentService.PostWorkUnitResultTask(
        agentId,
        workUnitId,
        targetId,
        shellOutputDTO
    );

    injectServiceToResubmissionTask(task);

    List<QueuedAgentService.PostWorkUnitResultTask> splitTasks = task.splitTask();
    assertEquals(1, splitTasks.size());
    assertEquals(task.agentId, splitTasks.get(0).agentId);
    assertEquals(task.hostId, splitTasks.get(0).hostId);
    assertEquals(task.workUnitId, splitTasks.get(0).workUnitId);
    assertEquals(task.shellOutputDTO, splitTasks.get(0).shellOutputDTO);
    assertNull(splitTasks.get(0).service);
  }

  // *******
  // ** PostPushDataResultTask
  // *******
  @Test
  public void postPushDataResultTaskExecuteCallsAppropriateService() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();

    long now = System.currentTimeMillis();

    String format = "unitTestFormat";

    List<String> pretendPushDataList = new ArrayList<String>();
    pretendPushDataList.add("string line 1");
    pretendPushDataList.add("string line 2");

    String pretendPushData = GraphiteStringHandler.joinPushData(pretendPushDataList);

    QueuedAgentService.PostPushDataResultTask task = new QueuedAgentService.PostPushDataResultTask(
        agentId,
        workUnitId,
        now,
        format,
        pretendPushData
    );

    injectServiceToResubmissionTask(task);

    EasyMock.expect(mockAgentAPI.postPushData(agentId, workUnitId, now, format, pretendPushData))
        .andReturn(Response.ok().build()).once();

    EasyMock.replay(mockAgentAPI);

    task.execute(null);

    EasyMock.verify(mockAgentAPI);
  }

  @Test
  public void postPushDataResultTaskExecuteServiceReturns406ThrowsException() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();

    long now = System.currentTimeMillis();

    String format = "unitTestFormat";

    List<String> pretendPushDataList = new ArrayList<String>();
    pretendPushDataList.add("string line 1");
    pretendPushDataList.add("string line 2");

    String pretendPushData = GraphiteStringHandler.joinPushData(pretendPushDataList);

    QueuedAgentService.PostPushDataResultTask task = new QueuedAgentService.PostPushDataResultTask(
        agentId,
        workUnitId,
        now,
        format,
        pretendPushData
    );

    injectServiceToResubmissionTask(task);

    EasyMock.expect(mockAgentAPI.postPushData(agentId, workUnitId, now, format, pretendPushData))
        .andReturn(Response.status(Response.Status.NOT_ACCEPTABLE).build()).once();

    EasyMock.replay(mockAgentAPI);

    boolean exceptionThrown = false;

    try {
      task.execute(null);
    } catch (RejectedExecutionException e) {
      exceptionThrown = true;
    }

    Assert.assertTrue(exceptionThrown);
    EasyMock.verify(mockAgentAPI);
  }

  @Test
  public void postPushDataResultTaskExecuteServiceReturns413ThrowsException() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();

    long now = System.currentTimeMillis();

    String format = "unitTestFormat";

    List<String> pretendPushDataList = new ArrayList<String>();
    pretendPushDataList.add("string line 1");
    pretendPushDataList.add("string line 2");

    String pretendPushData = GraphiteStringHandler.joinPushData(pretendPushDataList);

    QueuedAgentService.PostPushDataResultTask task = new QueuedAgentService.PostPushDataResultTask(
        agentId,
        workUnitId,
        now,
        format,
        pretendPushData
    );

    injectServiceToResubmissionTask(task);

    EasyMock.expect(mockAgentAPI.postPushData(agentId, workUnitId, now, format, pretendPushData))
        .andReturn(Response.status(Response.Status.REQUEST_ENTITY_TOO_LARGE).build()).once();

    EasyMock.replay(mockAgentAPI);

    boolean exceptionThrown = false;

    try {
      task.execute(null);
    } catch (QueuedPushTooLargeException e) {
      exceptionThrown = true;
    }

    Assert.assertTrue(exceptionThrown);
    EasyMock.verify(mockAgentAPI);
  }

  @Test
  public void postPushDataResultTaskSplitReturnsListOfOneIfOnlyOne() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();

    long now = System.currentTimeMillis();

    String format = "unitTestFormat";

    String str1 = "string line 1";

    List<String> pretendPushDataList = new ArrayList<String>();
    pretendPushDataList.add(str1);

    String pretendPushData = GraphiteStringHandler.joinPushData(pretendPushDataList);

    QueuedAgentService.PostPushDataResultTask task = new QueuedAgentService.PostPushDataResultTask(
        agentId,
        workUnitId,
        now,
        format,
        pretendPushData
    );

    List<PostPushDataResultTask> splitTasks = task.splitTask();
    assertEquals(1, splitTasks.size());

    String firstSplitDataString = splitTasks.get(0).getPushData();
    List<String> firstSplitData = GraphiteStringHandler.unjoinPushData(firstSplitDataString);

    assertEquals(1, firstSplitData.size());
  }

  @Test
  public void postPushDataResultTaskSplitTaskSplitsEvenly() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();

    long now = System.currentTimeMillis();

    String format = "unitTestFormat";

    String str1 = "string line 1";
    String str2 = "string line 2";
    String str3 = "string line 3";
    String str4 = "string line 4";

    List<String> pretendPushDataList = new ArrayList<String>();
    pretendPushDataList.add(str1);
    pretendPushDataList.add(str2);
    pretendPushDataList.add(str3);
    pretendPushDataList.add(str4);

    String pretendPushData = GraphiteStringHandler.joinPushData(pretendPushDataList);

    PostPushDataResultTask task = new PostPushDataResultTask(
        agentId,
        workUnitId,
        now,
        format,
        pretendPushData
    );

    List<PostPushDataResultTask> splitTasks = task.splitTask();
    assertEquals(2, splitTasks.size());

    String firstSplitDataString = splitTasks.get(0).getPushData();
    List<String> firstSplitData = GraphiteStringHandler.unjoinPushData(firstSplitDataString);
    assertEquals(2, firstSplitData.size());

    String secondSplitDataString = splitTasks.get(1).getPushData();
    List<String> secondSplitData = GraphiteStringHandler.unjoinPushData(secondSplitDataString);
    assertEquals(2, secondSplitData.size());

    // and all the data is the same...
    for (ResubmissionTask taskUnderTest : splitTasks) {
      PostPushDataResultTask taskUnderTestCasted = (PostPushDataResultTask) taskUnderTest;
      assertEquals(agentId, taskUnderTestCasted.getAgentId());
      assertEquals(workUnitId, taskUnderTestCasted.getWorkUnitId());
      assertEquals(now, (long) taskUnderTestCasted.getCurrentMillis());
      assertEquals(format, taskUnderTestCasted.getFormat());
    }

    // first list should have the first 2 strings
    assertEquals(GraphiteStringHandler.joinPushData(Arrays.asList(str1, str2)), firstSplitDataString);
    // second list should have the last 2
    assertEquals(GraphiteStringHandler.joinPushData(Arrays.asList(str3, str4)), secondSplitDataString);

  }

  @Test
  public void postPushDataResultTaskSplitsRoundsUpToLastElement() {
    /* its probably sufficient to test that all the points get included in the split
    as opposed to ensuring how they get split */

    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();

    long now = System.currentTimeMillis();

    String format = "unitTestFormat";

    String str1 = "string line 1";
    String str2 = "string line 2";
    String str3 = "string line 3";
    String str4 = "string line 4";
    String str5 = "string line 5";

    List<String> pretendPushDataList = new ArrayList<String>();
    pretendPushDataList.add(str1);
    pretendPushDataList.add(str2);
    pretendPushDataList.add(str3);
    pretendPushDataList.add(str4);
    pretendPushDataList.add(str5);

    String pretendPushData = GraphiteStringHandler.joinPushData(pretendPushDataList);

    PostPushDataResultTask task = new PostPushDataResultTask(
        agentId,
        workUnitId,
        now,
        format,
        pretendPushData
    );

    List<PostPushDataResultTask> splitTasks = task.splitTask();
    assertEquals(2, splitTasks.size());

    String firstSplitDataString = splitTasks.get(0).getPushData();
    List<String> firstSplitData = GraphiteStringHandler.unjoinPushData(firstSplitDataString);

    assertEquals(2, firstSplitData.size());

    String secondSplitDataString = splitTasks.get(1).getPushData();
    List<String> secondSplitData = GraphiteStringHandler.unjoinPushData(secondSplitDataString);

    assertEquals(3, secondSplitData.size());
  }
}

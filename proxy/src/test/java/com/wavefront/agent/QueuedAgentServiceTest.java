package com.wavefront.agent;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RecyclableRateLimiter;

import com.squareup.tape.TaskInjector;
import com.wavefront.agent.QueuedAgentService.PostPushDataResultTask;
import com.wavefront.agent.handlers.LineDelimitedUtils;
import com.wavefront.api.WavefrontAPI;

import net.jcip.annotations.NotThreadSafe;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.core.Response;

import io.netty.util.internal.StringUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Andrew Kao (andrew@wavefront.com)
 */
@NotThreadSafe
public class QueuedAgentServiceTest {

  private QueuedAgentService queuedAgentService;
  private WavefrontAPI mockAgentAPI;
  private UUID newAgentId;

  @Before
  public void testSetup() throws IOException {
    mockAgentAPI = EasyMock.createMock(WavefrontAPI.class);
    newAgentId = UUID.randomUUID();

    int retryThreads = 1;
    QueuedAgentService.setMinSplitBatchSize(2);
    queuedAgentService = new QueuedAgentService(mockAgentAPI, "unitTestBuffer", retryThreads,
        Executors.newScheduledThreadPool(retryThreads + 1, new ThreadFactory() {

          private AtomicLong counter = new AtomicLong();

          @Override
          public Thread newThread(Runnable r) {
            Thread toReturn = new Thread(r);
            toReturn.setName("unit test submission worker: " + counter.getAndIncrement());
            return toReturn;
          }
        }), true, newAgentId, false, (RecyclableRateLimiter) null, StringUtil.EMPTY_STRING);
    queuedAgentService.start();
  }
  // post sourcetag metadata

  /**
   * This test will try to delete a source tag and verify it works properly.
   *
   * @throws Exception
   */
  @Test
  public void postSourceTagDataPoint() throws Exception {
    String id = "localhost";
    String tagValue = "sourceTag1";
    EasyMock.expect(mockAgentAPI.removeTag(id, StringUtils.EMPTY, tagValue)).andReturn(Response.ok().build()).once();
    EasyMock.replay(mockAgentAPI);
    Response response = queuedAgentService.removeTag(id, StringUtils.EMPTY, tagValue);
    EasyMock.verify(mockAgentAPI);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
  }

  /**
   * This test will try to delete a source tag, but makes sure it goes to the queue instead.
   */
  @Test
  public void postSourceTagIntoQueue() {
    String id = "localhost";
    String tagValue = "sourceTag1";
    Response response = queuedAgentService.removeTag(id, tagValue, true);
    assertEquals(Response.Status.NOT_ACCEPTABLE.getStatusCode(), response.getStatus());
    assertEquals(1, queuedAgentService.getQueuedSourceTagTasksCount());
  }

  /**
   * This test will delete a description and verifies that the server side api was called properly
   *
   * @throws Exception
   */
  @Test
  public void removeSourceDescription() throws Exception {
    String id = "dummy";
    EasyMock.expect(mockAgentAPI.removeDescription(id, StringUtils.EMPTY)).andReturn(Response.ok().build()).once();
    EasyMock.replay(mockAgentAPI);
    Response response = queuedAgentService.removeDescription(id, false);
    EasyMock.verify(mockAgentAPI);
    assertEquals("Response code was incorrect.", Response.Status.OK.getStatusCode(), response
        .getStatus());
  }

  /**
   * This test will add a description and make sure it goes into the queue instead of going to
   * the server api directly.
   *
   * @throws Exception
   */
  @Test
  public void postSourceDescriptionIntoQueue() throws Exception {
    String id = "localhost";
    String desc = "A Description";
    Response response = queuedAgentService.setDescription(id, desc, true);
    assertEquals("Response code did not match", Response.Status.NOT_ACCEPTABLE.getStatusCode(),
        response.getStatus());
    assertEquals("No task found in the backlog queue", 1, queuedAgentService
        .getQueuedSourceTagTasksCount());
  }

  /**
   * This test will add 3 source tags and verify that the server api is called properly.
   *
   * @throws Exception
   */
  @Test
  public void postSourceTagsDataPoint() throws Exception {
    String id = "dummy";
    String tags[] = new String[]{"tag1", "tag2", "tag3"};
    EasyMock.expect(mockAgentAPI.setTags(id, StringUtils.EMPTY,
        Arrays.asList(tags))).andReturn(Response.ok().build()).once();
    EasyMock.replay(mockAgentAPI);
    Response response = queuedAgentService.setTags(id, Arrays.asList(tags), false);
    EasyMock.verify(mockAgentAPI);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
  }

  /**
   * This test is used to add a description to the source and verify that the server api is
   * called accurately.
   *
   * @throws Exception
   */
  @Test
  public void postSourceDescriptionData() throws Exception {
    String id = "dummy";
    String desc = "A Description";
    EasyMock.expect(mockAgentAPI.setDescription(id, StringUtils.EMPTY, desc)).andReturn(Response.ok()
        .build()).once();
    EasyMock.replay(mockAgentAPI);
    Response response = queuedAgentService.setDescription(id, desc, false);
    EasyMock.verify(mockAgentAPI);
    assertEquals("Response code did not match.", Response.Status.OK.getStatusCode(),
        response.getStatus());
  }

  /**
   * This test will try to delete a source tag and mock a 406 response from the server. The delete
   * request should get queued and tried again.
   *
   * @throws Exception
   */
  @Test
  public void postSourceTagAndHandle406Response() throws Exception{
    // set up the mocks
    String id = "localhost";
    String tagValue = "sourceTag1";
    EasyMock.expect(mockAgentAPI.removeTag(id, StringUtils.EMPTY, tagValue)).andReturn(Response.status(Response
        .Status.NOT_ACCEPTABLE).build())
        .once();
    EasyMock.expect(mockAgentAPI.removeTag(id, StringUtils.EMPTY, tagValue)).andReturn(Response.status(Response
        .Status.OK).build()).once();
    EasyMock.replay(mockAgentAPI);
    // call the api
    Response response = queuedAgentService.removeTag(id, tagValue, false);
    // verify
    assertEquals(Response.Status.NOT_ACCEPTABLE.getStatusCode(), response.getStatus());
    assertEquals(1, queuedAgentService.getQueuedSourceTagTasksCount());
    // wait for a few seconds for the task to be picked up from the queue
    TimeUnit.SECONDS.sleep(5);
    EasyMock.verify(mockAgentAPI);
    assertEquals(0, queuedAgentService.getQueuedSourceTagTasksCount());
  }

  /**
   * This test will add source tags and mock a 406 response from the server. The add requests
   * should get queued and tried out again.
   *
   * @throws Exception
   */
  @Test
  public void postSourceTagsAndHandle406Response() throws Exception {
    String id = "dummy";
    String tags[] = new String[]{"tag1", "tag2", "tag3"};
    EasyMock.expect(mockAgentAPI.setTags(id, StringUtils.EMPTY, Arrays.asList(tags))).andReturn(
        Response.status(Response.Status.NOT_ACCEPTABLE).build()).once();
    EasyMock.expect(mockAgentAPI.setTags(id, StringUtils.EMPTY, Arrays.asList(tags))).andReturn(
        Response.status(Response.Status.OK).build()).once();
    EasyMock.replay(mockAgentAPI);
    Response response = queuedAgentService.setTags(id, Arrays.asList(tags), false);
    // verify
    assertEquals(Response.Status.NOT_ACCEPTABLE.getStatusCode(), response.getStatus());
    assertEquals(1, queuedAgentService.getQueuedSourceTagTasksCount());
    // wait for a few seconds for the task to be picked up from the queue
    TimeUnit.SECONDS.sleep(5);
    EasyMock.verify(mockAgentAPI);
    assertEquals(0, queuedAgentService.getQueuedSourceTagTasksCount());
  }

  /**
   * This test will add source description and mock a 406 response from the server. The add
   * requests should get queued and tried again.
   *
   * @throws Exception
   */
  @Test
  public void postSourceDescriptionAndHandle406Response() throws Exception {
    String id = "dummy";
    String description = "A Description";
    EasyMock.expect(mockAgentAPI.setDescription(id, StringUtils.EMPTY, description)).andReturn(Response
        .status
        (Response.Status.NOT_ACCEPTABLE).build()).once();
    EasyMock.expect(mockAgentAPI.setDescription(id, StringUtils.EMPTY, description)).andReturn(Response
        .status
        (Response.Status.OK).build()).once();
    EasyMock.replay(mockAgentAPI);
    Response response = queuedAgentService.setDescription(id, description, false);
    // verify
    assertEquals(Response.Status.NOT_ACCEPTABLE.getStatusCode(), response.getStatus());
    assertEquals(1, queuedAgentService.getQueuedSourceTagTasksCount());
    // wait for a few seconds for the task to be picked up from the queue
    TimeUnit.SECONDS.sleep(5);
    EasyMock.verify(mockAgentAPI);
    assertEquals(0, queuedAgentService.getQueuedSourceTagTasksCount());
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

    String pretendPushData = LineDelimitedUtils.joinPushData(pretendPushDataList);

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

    String pretendPushData = LineDelimitedUtils.joinPushData(pretendPushDataList);

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

    QueuedAgentService.setMinSplitBatchSize(1);
    String pretendPushData = LineDelimitedUtils.joinPushData(pretendPushDataList);

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

    String pretendPushData = LineDelimitedUtils.joinPushData(pretendPushDataList);
    QueuedAgentService.setMinSplitBatchSize(1);

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
        task.currentAgentId = newAgentId;
      }
    }.injectMembers(task);
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

    String pretendPushData = LineDelimitedUtils.joinPushData(pretendPushDataList);

    QueuedAgentService.PostPushDataResultTask task = new QueuedAgentService.PostPushDataResultTask(
        agentId,
        workUnitId,
        now,
        format,
        pretendPushData
    );

    injectServiceToResubmissionTask(task);

    EasyMock.expect(mockAgentAPI.postPushData(newAgentId, workUnitId, now, format, pretendPushData))
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

    String pretendPushData = LineDelimitedUtils.joinPushData(pretendPushDataList);

    QueuedAgentService.PostPushDataResultTask task = new QueuedAgentService.PostPushDataResultTask(
        agentId,
        workUnitId,
        now,
        format,
        pretendPushData
    );

    injectServiceToResubmissionTask(task);

    EasyMock.expect(mockAgentAPI.postPushData(newAgentId, workUnitId, now, format, pretendPushData))
        .andReturn(Response.status(Response.Status.NOT_ACCEPTABLE).build()).once();

    EasyMock.replay(mockAgentAPI);

    boolean exceptionThrown = false;

    try {
      task.execute(null);
    } catch (RejectedExecutionException e) {
      exceptionThrown = true;
    }

    assertTrue(exceptionThrown);
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

    String pretendPushData = LineDelimitedUtils.joinPushData(pretendPushDataList);

    QueuedAgentService.PostPushDataResultTask task = new QueuedAgentService.PostPushDataResultTask(
        agentId,
        workUnitId,
        now,
        format,
        pretendPushData
    );

    injectServiceToResubmissionTask(task);

    EasyMock.expect(mockAgentAPI.postPushData(newAgentId, workUnitId, now, format, pretendPushData))
        .andReturn(Response.status(Response.Status.REQUEST_ENTITY_TOO_LARGE).build()).once();

    EasyMock.replay(mockAgentAPI);

    boolean exceptionThrown = false;

    try {
      task.execute(null);
    } catch (QueuedPushTooLargeException e) {
      exceptionThrown = true;
    }

    assertTrue(exceptionThrown);
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

    String pretendPushData = LineDelimitedUtils.joinPushData(pretendPushDataList);

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
    List<String> firstSplitData = unjoinPushData(firstSplitDataString);

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

    String pretendPushData = LineDelimitedUtils.joinPushData(pretendPushDataList);

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
    List<String> firstSplitData = unjoinPushData(firstSplitDataString);
    assertEquals(2, firstSplitData.size());

    String secondSplitDataString = splitTasks.get(1).getPushData();
    List<String> secondSplitData = unjoinPushData(secondSplitDataString);
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
    assertEquals(LineDelimitedUtils.joinPushData(Arrays.asList(str1, str2)), firstSplitDataString);
    // second list should have the last 2
    assertEquals(LineDelimitedUtils.joinPushData(Arrays.asList(str3, str4)), secondSplitDataString);

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

    String pretendPushData = LineDelimitedUtils.joinPushData(pretendPushDataList);

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
    List<String> firstSplitData = unjoinPushData(firstSplitDataString);

    assertEquals(3, firstSplitData.size());

    String secondSplitDataString = splitTasks.get(1).getPushData();
    List<String> secondSplitData = unjoinPushData(secondSplitDataString);

    assertEquals(2, secondSplitData.size());
  }

  @Test
  public void splitIntoTwoTest() {
    UUID agentId = UUID.randomUUID();
    UUID workUnitId = UUID.randomUUID();

    long now = System.currentTimeMillis();

    String format = "unitTestFormat";

    for (int numTestStrings = 2; numTestStrings <= 51; numTestStrings += 1) {
      List<String> pretendPushDataList = new ArrayList<String>();
      for (int i = 0; i < numTestStrings; i++) {
        pretendPushDataList.add(RandomStringUtils.randomAlphabetic(6));
      }

      QueuedAgentService.setMinSplitBatchSize(1);
      String pretendPushData = LineDelimitedUtils.joinPushData(pretendPushDataList);

      PostPushDataResultTask task = new PostPushDataResultTask(
          agentId,
          workUnitId,
          now,
          format,
          pretendPushData
      );

      List<PostPushDataResultTask> splitTasks = task.splitTask();
      assertEquals(2, splitTasks.size());
      Set<String> splitData = Sets.newHashSet();
      for (PostPushDataResultTask taskN : splitTasks) {
        List<String> dataStrings = unjoinPushData(taskN.getPushData());
        splitData.addAll(dataStrings);
        assertTrue(dataStrings.size() <= numTestStrings / 2 + 1);
      }
      assertEquals(Sets.newHashSet(pretendPushDataList), splitData);
    }
  }

  private static List<String> unjoinPushData(String pushData) {
    return Arrays.asList(StringUtils.split(pushData, "\n"));
  }
}

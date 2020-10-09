package com.wavefront.agent.queueing;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.squareup.tape2.QueueFile;
import com.wavefront.common.Pair;

import static com.wavefront.agent.queueing.ConcurrentShardedQueueFile.incrementFileName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author vasily@wavefront.com
 */
public class ConcurrentShardedQueueFileTest {
  private static final Random RANDOM = new Random();

  @Test
  public void nextFileNameTest() {
    assertEquals("points.2878.1_0000", incrementFileName("points.2878.1", ".spool"));
    assertEquals("points.2878.1.spool_0000", incrementFileName("points.2878.1.spool", ".spool"));
    assertEquals("points.2878.1.spool_0001", incrementFileName("points.2878.1.spool_0000", ".spool"));
    assertEquals("points.2878.1.spool_0002", incrementFileName("points.2878.1.spool_0001", ".spool"));
    assertEquals("points.2878.1.spool_000a", incrementFileName("points.2878.1.spool_0009", ".spool"));
    assertEquals("points.2878.1.spool_0010", incrementFileName("points.2878.1.spool_000f", ".spool"));
    assertEquals("points.2878.1.spool_0100", incrementFileName("points.2878.1.spool_00ff", ".spool"));
    assertEquals("points.2878.1.spool_ffff", incrementFileName("points.2878.1.spool_fffe", ".spool"));
    assertEquals("points.2878.1.spool_0000", incrementFileName("points.2878.1.spool_ffff", ".spool"));
  }

  @Test
  public void testConcurrency() throws Exception {
    File file = new File(File.createTempFile("proxyConcurrencyTest", null).getPath() + ".spool");
    ConcurrentShardedQueueFile queueFile = new ConcurrentShardedQueueFile(file.getCanonicalPath(),
        ".spool", 1024 * 1024, s -> new TapeQueueFile(new QueueFile.Builder(new File(s)).build()));
    Queue<Pair<Integer, Byte>> taskCheatSheet = new ArrayDeque<>();
    System.out.println(queueFile.shards.size());
    AtomicLong tasksGenerated = new AtomicLong();
    AtomicLong nanosAdd = new AtomicLong();
    AtomicLong nanosGet = new AtomicLong();
    while (queueFile.shards.size() < 4) {
      byte[] task = randomTask();
      queueFile.add(task);
      taskCheatSheet.add(Pair.of(task.length, task[0]));
      tasksGenerated.incrementAndGet();
    }
    AtomicBoolean done = new AtomicBoolean(false);
    AtomicBoolean fail = new AtomicBoolean(false);
    Runnable addTask = () -> {
      int delay = 0;
      while (!done.get() && !fail.get()) {
        try {
          byte[] task = randomTask();
          long start = System.nanoTime();
          queueFile.add(task);
          nanosAdd.addAndGet(System.nanoTime() - start);
          taskCheatSheet.add(Pair.of(task.length, task[0]));
          tasksGenerated.incrementAndGet();
          Thread.sleep(delay / 1000);
          delay++;
        } catch (Exception e) {
          e.printStackTrace();
          fail.set(true);
        }
      }
    };
    Runnable getTask = () -> {
      int delay = 2000;
      while (!taskCheatSheet.isEmpty() && !fail.get()) {
        try {
          long start = System.nanoTime();
          Pair<Integer, Byte> taskData = taskCheatSheet.remove();
          byte[] task = queueFile.peek();
          queueFile.remove();
          nanosGet.addAndGet(System.nanoTime() - start);
          if (taskData._1 != task.length) {
            System.out.println("Data integrity fail! Expected: " + taskData._1 +
                    " bytes, got " + task.length + " bytes");
            fail.set(true);
          }
          for (byte b : task) {
            if (taskData._2 != b) {
              System.out.println("Data integrity fail! Expected " + taskData._2 + ", got " + b);
              fail.set(true);
            }
          }
          Thread.sleep(delay / 500);
          if (delay > 0) delay--;
        } catch (Exception e) {
          e.printStackTrace();
          fail.set(true);
        }
      }
      done.set(true);
    };
    ExecutorService executor = Executors.newFixedThreadPool(2);
    long start = System.nanoTime();
    Future<?> addFuture = executor.submit(addTask);
    Future<?> getFuture = executor.submit(getTask);
    addFuture.get();
    getFuture.get();
    assertFalse(fail.get());
    System.out.println("Tasks generated: " + tasksGenerated.get());
    System.out.println("Real time (ms) = " + (System.nanoTime() - start) / 1_000_000);
    System.out.println("Add + remove time (ms) = " + (nanosGet.get() + nanosAdd.get()) / 1_000_000);
    System.out.println("Add time (ms) = " + nanosAdd.get() / 1_000_000);
    System.out.println("Remove time (ms) = " + nanosGet.get() / 1_000_000);
  }

  private byte[] randomTask() {
    int size = RANDOM.nextInt(32 * 1024) + 1;
    byte[] result = new byte[size];
    RANDOM.nextBytes(result);
    Arrays.fill(result, result[0]);
    return result;
  }
}
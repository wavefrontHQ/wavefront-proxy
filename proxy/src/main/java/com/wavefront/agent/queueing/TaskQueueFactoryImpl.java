package com.wavefront.agent.queueing;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.squareup.tape2.QueueFile;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.metrics.ExpectedAgentMetric;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * A caching implementation of a {@link TaskQueueFactory}.
 *
 * @author vasily@wavefront.com.
 */
public class TaskQueueFactoryImpl implements TaskQueueFactory {
  private static final Logger logger =
      Logger.getLogger(TaskQueueFactoryImpl.class.getCanonicalName());
  private final Map<HandlerKey, Map<Integer, TaskQueue<?>>> taskQueues = new ConcurrentHashMap<>();

  private final String bufferFile;
  private final boolean purgeBuffer;

  /**
   * @param bufferFile  Path prefix for queue file names.
   * @param purgeBuffer Whether buffer files should be nuked before starting (this may cause data
   *                    loss if queue files are not empty).
   */
  public TaskQueueFactoryImpl(String bufferFile, boolean purgeBuffer) {
    this.bufferFile = bufferFile;
    this.purgeBuffer = purgeBuffer;

    Metrics.newGauge(ExpectedAgentMetric.BUFFER_BYTES_LEFT.metricName,
        new Gauge<Long>() {
          @Override
          public Long value() {
            try {
              long availableBytes = taskQueues.values().stream().
                  flatMap(x -> x.values().stream()).
                  map(TaskQueue::getAvailableBytes).
                  filter(Objects::nonNull).mapToLong(x -> x).sum();

              File bufferDirectory = new File(bufferFile).getAbsoluteFile();
              while (bufferDirectory != null && bufferDirectory.getUsableSpace() == 0) {
                bufferDirectory = bufferDirectory.getParentFile();
              }
              if (bufferDirectory != null) {
                return bufferDirectory.getUsableSpace() + availableBytes;
              }
            } catch (Throwable t) {
              logger.warning("cannot compute remaining space in buffer file partition: " + t);
            }
            return null;
          }
        }
    );
  }

  public <T extends DataSubmissionTask<T>> TaskQueue<T> getTaskQueue(@Nonnull HandlerKey key,
                                                                     int threadNum) {
    //noinspection unchecked
    TaskQueue<T> taskQueue = (TaskQueue<T>) taskQueues.computeIfAbsent(key, x -> new TreeMap<>()).
        computeIfAbsent(threadNum, x -> createTaskQueue(key, threadNum));
    try {
      // check if queue is closed and re-create if it is.
      taskQueue.peek();
    } catch (IllegalStateException e) {
      taskQueue = createTaskQueue(key, threadNum);
      taskQueues.get(key).put(threadNum, taskQueue);
    }
    return taskQueue;
  }

  private <T extends DataSubmissionTask<T>> TaskQueue<T> createTaskQueue(
      @Nonnull HandlerKey handlerKey, int threadNum) {
    String fileName = bufferFile + "." + handlerKey.getEntityType().toString() + "." +
        handlerKey.getHandle() + "." + threadNum;
    String lockFileName = fileName + ".lck";
    String spoolFileName = fileName + ".spool";
    // Having two proxy processes write to the same buffer file simultaneously causes buffer
    // file corruption. To prevent concurrent access from another process, we try to obtain
    // exclusive access to a .lck file. trylock() is platform-specific so there is no
    // iron-clad guarantee, but it works well in most cases.
    try {
      File lockFile = new File(lockFileName);
      if (lockFile.exists()) {
        Preconditions.checkArgument(true, lockFile.delete());
      }
      FileChannel channel = new RandomAccessFile(lockFile, "rw").getChannel();
      // fail if tryLock() returns null (lock couldn't be acquired)
      Preconditions.checkNotNull(channel.tryLock());
    } catch (Exception e) {
      logger.severe("WF-005: Error requesting exclusive access to the buffer " +
          "lock file " + lockFileName + " - please make sure that no other processes " +
          "access this file and restart the proxy");
      return new TaskQueueStub<>();
    }
    try {
      File buffer = new File(spoolFileName);
      if (purgeBuffer) {
        if (buffer.delete()) {
          logger.warning("Retry buffer has been purged: " + spoolFileName);
        }
      }
      // TODO: allow configurable compression types and levels
      return new SynchronizedTaskQueueWithMetrics<>(new FileBasedTaskQueue<>(
          new QueueFile.Builder(buffer).build(),
          new RetryTaskConverter<T>(handlerKey.getHandle(), TaskConverter.CompressionType.LZ4)),
          "buffer", ImmutableMap.of("port", handlerKey.getHandle()), handlerKey.getEntityType());
    } catch (Exception e) {
      logger.severe("WF-006: Unable to open or create queue file " + spoolFileName + ": " +
          e.getMessage());
      return new TaskQueueStub<>();
    }
  }
}

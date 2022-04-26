package com.wavefront.agent.queueing;

import com.google.common.collect.ImmutableMap;
import com.squareup.tape2.QueueFile;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.common.Pair;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.metrics.ExpectedAgentMetric;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
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
  private final List<Pair<FileChannel, FileLock>> taskQueuesLocks = new ArrayList<>();

  private final String bufferFile;
  private final boolean purgeBuffer;
  private final boolean disableSharding;
  private final int shardSize;

  private static final Counter bytesWritten = Metrics.newCounter(new TaggedMetricName("buffer",
      "bytes-written"));
  private static final Counter ioTimeWrites = Metrics.newCounter(new TaggedMetricName("buffer",
      "io-time-writes"));

  /**
   * @param bufferFile      File name prefix for queue file names.
   * @param purgeBuffer     Whether buffer files should be nuked before starting (this may cause
   *                        data loss if queue files are not empty).
   * @param disableSharding disable buffer sharding (use single file)
   * @param shardSize       target shard size (in MBytes)
   */
  public TaskQueueFactoryImpl(String bufferFile, boolean purgeBuffer,
                              boolean disableSharding, int shardSize) {
    this.bufferFile = bufferFile;
    this.purgeBuffer = purgeBuffer;
    this.disableSharding = disableSharding;
    this.shardSize = shardSize;

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
      FileChannel channel = new RandomAccessFile(lockFile, "rw").getChannel();
      FileLock lock = channel.tryLock();
      logger.severe("lockFile: "+lockFile);
      if (lock == null) {
        channel.close();
        throw new OverlappingFileLockException();
      }
      logger.severe("lock isValid: "+lock.isValid()+" - isShared: "+lock.isShared());
      taskQueuesLocks.add(new Pair<>(channel, lock));
    } catch (SecurityException e) {
      logger.severe("Error writing to the buffer lock file " + lockFileName +
          " - please make sure write permissions are correct for this file path and restart the " +
          "proxy: " + e);
      return new TaskQueueStub<>();
    } catch (OverlappingFileLockException e) {
      logger.severe("Error requesting exclusive access to the buffer " +
          "lock file " + lockFileName + " - please make sure that no other processes " +
          "access this file and restart the proxy: " + e);
      return new TaskQueueStub<>();
    } catch (IOException e) {
      logger.severe("Error requesting access to buffer lock file " + lockFileName + " Channel is " +
          "closed or an I/O error has occurred - please restart the proxy: " + e);
      return new TaskQueueStub<>();
    }
    try {
      File buffer = new File(spoolFileName);
      if (purgeBuffer) {
        if (buffer.delete()) {
          logger.warning("Retry buffer has been purged: " + spoolFileName);
        }
      }
      BiConsumer<Integer, Long> statsUpdater = (bytes, millis) -> {
        bytesWritten.inc(bytes);
        ioTimeWrites.inc(millis);
      };
      com.wavefront.agent.queueing.QueueFile queueFile = disableSharding ?
          new ConcurrentQueueFile(new TapeQueueFile(new QueueFile.Builder(
              new File(spoolFileName)).build(), statsUpdater)) :
          new ConcurrentShardedQueueFile(spoolFileName, ".spool", shardSize * 1024 * 1024,
              s -> new TapeQueueFile(new QueueFile.Builder(new File(s)).build(), statsUpdater));
      // TODO: allow configurable compression types and levels
      return new InstrumentedTaskQueueDelegate<>(new FileBasedTaskQueue<>(queueFile,
          new RetryTaskConverter<T>(handlerKey.getHandle(), TaskConverter.CompressionType.LZ4)),
          "buffer", ImmutableMap.of("port", handlerKey.getHandle()), handlerKey.getEntityType());
    } catch (Exception e) {
      logger.severe("WF-006: Unable to open or create queue file " + spoolFileName + ": " +
          e.getMessage());
      return new TaskQueueStub<>();
    }
  }
}

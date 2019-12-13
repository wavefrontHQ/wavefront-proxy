package com.wavefront.agent.queueing;

import com.google.common.base.Preconditions;
import com.squareup.tape2.ObjectQueue;
import com.squareup.tape2.QueueFile;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * A caching implementation of a {@link TaskQueueFactory}.
 *
 * @author vasily@wavefront.com.
 */
public class TaskQueueFactoryImpl implements TaskQueueFactory {
  private static final Logger logger =
      Logger.getLogger(TaskQueueFactoryImpl.class.getCanonicalName());
  private final Map<HandlerKey, Map<Integer, TaskQueue<?>>> taskQueues = new HashMap<>();

  private final String bufferFile;
  private final boolean purgeBuffer;

  /**
   * Create a new instance.
   *
   * @param bufferFile  Path prefix for queue file names.
   * @param purgeBuffer Whether buffer files should be nuked before starting (this may cause data
   *                    loss if queue files are not empty).
   */
  public TaskQueueFactoryImpl(String bufferFile, boolean purgeBuffer) {
    this.bufferFile = bufferFile;
    this.purgeBuffer = purgeBuffer;
  }

  public <T extends DataSubmissionTask<T>> TaskQueue<T> getTaskQueue(@NotNull HandlerKey handlerKey,
                                                                     int threadNum) {
    //noinspection unchecked
    return (TaskQueue<T>) taskQueues.computeIfAbsent(handlerKey, x -> new TreeMap<>()).
        computeIfAbsent(threadNum, x -> {
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
            logger.severe("WF-005: Error requesting exclusive access to the buffer lock file " +
                lockFileName + " - please make sure that no other processes access this file " +
                "and restart the proxy");
            System.exit(-1);
          }
          try {
            File buffer = new File(spoolFileName);
            if (purgeBuffer) {
              if (buffer.delete()) {
                logger.warning("Retry buffer has been purged: " + spoolFileName);
              }
            }
            return new DataSubmissionQueue<>(ObjectQueue.create(
                new QueueFile.Builder(buffer).build(),
                new RetryTaskConverter<T>(handlerKey.getHandle(), true)), // TODO (VV): enable compression
                handlerKey.getHandle(), handlerKey.getEntityType());
          } catch (Exception e) {
            logger.severe("WF-006: Unable to open or create queue file " + spoolFileName);
            System.exit(-1);
          }
          return null;
        });
  }
}

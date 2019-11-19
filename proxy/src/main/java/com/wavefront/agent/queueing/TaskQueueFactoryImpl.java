package com.wavefront.agent.queueing;

import com.google.common.base.Preconditions;
import com.squareup.tape2.ObjectQueue;
import com.squareup.tape2.QueueFile;
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
 *
 * @author vasily@wavefront.com.
 */
public class TaskQueueFactoryImpl implements TaskQueueFactory {
  private static final Logger logger =
      Logger.getLogger(TaskQueueFactoryImpl.class.getCanonicalName());
  private final Map<HandlerKey, Map<Integer, TaskQueue>> taskQueues = new HashMap<>();

  private final String bufferFile;
  private final boolean purgeBuffer;

  /**
   *
   *
   * @param bufferFile
   * @param purgeBuffer
   */
  public TaskQueueFactoryImpl(String bufferFile, boolean purgeBuffer) {
    this.bufferFile = bufferFile;
    this.purgeBuffer = purgeBuffer;
  }

  public TaskQueue getTaskQueue(@NotNull HandlerKey handlerKey, int threadNum) {
    return taskQueues.computeIfAbsent(handlerKey, x -> new TreeMap<>()).computeIfAbsent(threadNum,
        x -> {
          String fileName = bufferFile + "." + handlerKey.getEntityType().toString() + "." +
              handlerKey.getHandle() + "." + threadNum;
          // Having two proxy processes write to the same buffer file simultaneously causes buffer
          // file corruption. To prevent concurrent access from another process, we try to obtain
          // exclusive access to a .lck file. trylock() is platform-specific so there is no
          // iron-clad guarantee, but it works well in most cases.
          try {
            File lockFile = new File(fileName + ".lck");
            if (lockFile.exists()) {
              Preconditions.checkArgument(true, lockFile.delete());
            }
            FileChannel channel = new RandomAccessFile(lockFile, "rw").getChannel();
            // fail if tryLock() returns null (lock couldn't be acquired)
            Preconditions.checkNotNull(channel.tryLock());
          } catch (Exception e) {
            logger.severe("WF-005: Error requesting exclusive access to the buffer lock file " +
                fileName + ".lck - please make sure that no other processes access this file " +
                "and restart the proxy");
            System.exit(-1);
          }
          try {
            File buffer = new File(fileName);
            if (purgeBuffer) {
              if (buffer.delete()) {
                logger.warning("Retry buffer has been purged: " + fileName);
              }
            }
            return new DataSubmissionQueue<>(ObjectQueue.create(
                new QueueFile.Builder(buffer).build(),
                new RetryTaskConverter<>(handlerKey.getHandle(), true)), // TODO: enable compression
                handlerKey.getHandle(), handlerKey.getEntityType());
          } catch (Exception e) {
            logger.severe("WF-006: Unable to open or create queue file " + fileName);
            System.exit(-1);
          }
          return null;
        });
  }
}

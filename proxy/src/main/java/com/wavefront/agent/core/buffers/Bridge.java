package com.wavefront.agent.core.buffers;

import com.wavefront.agent.core.queues.QueueStats;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerMessagePlugin;

public class Bridge implements ActiveMQServerMessagePlugin {
  private static final Logger log = Logger.getLogger(Bridge.class.getCanonicalName());

  private final MemoryBuffer memoryBuffer;
  private final DiskBuffer diskBuffer;
  private final Timer checkDiskFull;

  public Bridge(MemoryBuffer memoryBuffer, DiskBuffer diskBuffer) {
    this.memoryBuffer = memoryBuffer;
    this.diskBuffer = diskBuffer;
    checkDiskFull = new Timer(); // TODO stop the timer on shutdown ?
    checkDiskFull.scheduleAtFixedRate(
        new TimerTask() {
          @Override
          public void run() {
            if (diskBuffer.isFull()) {
              memoryBuffer.disableBridge();
            } else {
              memoryBuffer.enableBridge();
            }
          }
        },
        TimeUnit.MINUTES.toMillis(1),
        TimeUnit.MINUTES.toMillis(1));
  }

  @Override
  public void messageAcknowledged(MessageReference ref, AckReason reason, ServerConsumer consumer)
      throws ActiveMQException {
    if (reason == AckReason.KILLED || reason == AckReason.EXPIRED) {
      String queue = ref.getQueue().getAddress().toString();
      //      log.info("-> queue:" + queue + " - " + reason + " - " + diskBuffer.isFull());
      CoreMessage msg = (CoreMessage) ref.getMessage().copy();
      String stringBody = msg.getReadOnlyBodyBuffer().readString();
      List<String> points = Arrays.asList(stringBody.split("\n"));
      QueueStats stats = QueueStats.get(queue);
      try {
        diskBuffer.sendPoints(queue, points);
        switch (reason) {
          case KILLED:
            stats.queuedFailed.inc(ref.getMessage().getIntProperty("points"));
            break;
          case EXPIRED:
            stats.queuedExpired.inc(ref.getMessage().getIntProperty("points"));
            break;
        }
      } catch (ActiveMQAddressFullException e) {
        // disk buffer full, we put the metrics bach to memory
        // and disable this.
        memoryBuffer.sendPoints(queue, points);
        memoryBuffer.disableBridge();
      } catch (Exception e) {
        log.log(Level.SEVERE, "Error deleting expired messages", e);
        throw new ActiveMQException("Error deleting expired messages. " + e.getMessage());
      }
    }
  }
}

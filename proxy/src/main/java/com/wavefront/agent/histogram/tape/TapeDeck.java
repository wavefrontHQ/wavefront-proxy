package com.wavefront.agent.histogram.tape;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.squareup.tape.FileObjectQueue;
import com.squareup.tape.InMemoryObjectQueue;
import com.squareup.tape.ObjectQueue;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Factory for Square Tape {@link ObjectQueue} instances for this agent.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class TapeDeck<T> {
  private static final Logger logger = Logger.getLogger(TapeDeck.class.getCanonicalName());

  private final LoadingCache<File, ObjectQueue<T>> queues;
  private final boolean doPersist;

  /**
   * @param converter payload (de-)/serializer.
   * @param doPersist whether to persist the queue
   */
  public TapeDeck(final FileObjectQueue.Converter<T> converter, boolean doPersist) {
    this.doPersist = doPersist;
    queues = CacheBuilder.newBuilder().build(new CacheLoader<File, ObjectQueue<T>>() {
      @Override
      public ObjectQueue<T> load(@NotNull File file) throws Exception {

        ObjectQueue<T> queue;

        if (doPersist) {

          // We need exclusive ownership of the file for this deck.
          // This is really no guarantee that we have exclusive access to the file (see e.g. goo.gl/i4S7ha)
          try {
            queue = new FileObjectQueue<>(file, converter);
            FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
            Preconditions.checkNotNull(channel.tryLock());
          } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while loading persisted Tape Queue for file " + file +
                ". Please move or delete the file and restart the agent.", e);
            System.exit(-1);
            queue = null;
          }
        } else {
          queue = new InMemoryObjectQueue<>();
        }

        return new ReportingObjectQueueWrapper<>(queue, file.getName());
      }
    });
  }

  @Nullable
  public ObjectQueue<T> getTape(@NotNull File f) {
    try {
      return queues.get(f);
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Error while loading " + f, e);
      throw new RuntimeException("Unable to provide ObjectQueue", e);
    }
  }

  @Override
  public String toString() {
    return "TapeDeck{" +
        "queues=" + queues +
        ", doPersist=" + doPersist +
        '}';
  }

  /**
   * Threadsafe ObjectQueue wrapper with add, remove and peek counters;
   */
  private static class ReportingObjectQueueWrapper<T> implements ObjectQueue<T> {
    private final ObjectQueue<T> backingQueue;
    private final Counter addCounter;
    private final Counter removeCounter;
    private final Counter peekCounter;

    ReportingObjectQueueWrapper(ObjectQueue<T> backingQueue, String title) {
      this.addCounter = Metrics.newCounter(new MetricName("tape." + title, "", "add"));
      this.removeCounter = Metrics.newCounter(new MetricName("tape." + title, "", "remove"));
      this.peekCounter = Metrics.newCounter(new MetricName("tape." + title, "", "peek"));
      Metrics.newGauge(new MetricName("tape." + title, "", "size"),
          new Gauge<Integer>() {
            @Override
            public Integer value() {
              return backingQueue.size();
            }
          });

      this.backingQueue = backingQueue;
    }

    @Override
    public int size() {
      synchronized (this) {
        return backingQueue.size();
      }
    }

    @Override
    public void add(T t) {
      addCounter.inc();
      synchronized (this) {
        backingQueue.add(t);
      }
    }

    @Override
    public T peek() {
      peekCounter.inc();
      synchronized (this) {
        return backingQueue.peek();
      }
    }

    @Override
    public void remove() {
      removeCounter.inc();
      synchronized (this) {
        backingQueue.remove();
      }
    }

    @Override
    public void setListener(Listener<T> listener) {
      synchronized (this) {
        backingQueue.setListener(listener);
      }
    }
  }
}

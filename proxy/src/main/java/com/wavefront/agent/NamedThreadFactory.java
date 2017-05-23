package com.wavefront.agent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.validation.constraints.NotNull;

/**
 * Simple thread factory to be used with Executors.newScheduledThreadPool that allows assigning name prefixes
 * to all pooled threads to simplify thread identification during troubleshooting.
 *
 * Created by vasily@wavefront.com on 3/16/17.
 */
public class NamedThreadFactory implements ThreadFactory{
  private final String threadNamePrefix;
  private final AtomicInteger counter = new AtomicInteger();

  public NamedThreadFactory(@NotNull String threadNamePrefix) {
    this.threadNamePrefix = threadNamePrefix;
  }

  @Override
  public Thread newThread(@NotNull Runnable r) {
    Thread toReturn = new Thread(r);
    toReturn.setName(threadNamePrefix + "-" + counter.getAndIncrement());
    return toReturn;
  }
}

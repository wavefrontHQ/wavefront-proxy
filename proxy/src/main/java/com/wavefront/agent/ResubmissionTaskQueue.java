package com.wavefront.agent;

import com.squareup.tape.ObjectQueue;
import com.squareup.tape.TaskInjector;
import com.squareup.tape.TaskQueue;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Vasily on 8/18/16.
 */
public class ResubmissionTaskQueue extends TaskQueue<ResubmissionTask> {

  // maintain a fair lock on the queue
  private ReentrantLock lock = new ReentrantLock(true);

  public ResubmissionTaskQueue(ObjectQueue<ResubmissionTask> objectQueue, TaskInjector<ResubmissionTask> taskInjector) {
    super(objectQueue, taskInjector);
  }

  public Lock getLockObject() {
    return lock;
  }

}

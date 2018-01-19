package com.wavefront.agent;

import com.squareup.tape.ObjectQueue;
import com.squareup.tape.TaskInjector;
import com.squareup.tape.TaskQueue;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe TaskQueue for holding ResubmissionTask objects
 *
 * @author vasily@wavefront.com
 */
public class ResubmissionTaskQueue extends TaskQueue<ResubmissionTask> {

  // maintain a fair lock on the queue
  private ReentrantLock queueLock = new ReentrantLock(true);

  public ResubmissionTaskQueue(ObjectQueue<ResubmissionTask> objectQueue, TaskInjector<ResubmissionTask> taskInjector) {
    super(objectQueue, taskInjector);
  }

  @Override
  public void add(ResubmissionTask task) {
    queueLock.lock();
    try {
      super.add(task);
    } finally {
      queueLock.unlock();
    }
  }

  @Override
  public ResubmissionTask peek() {
    ResubmissionTask task;
    queueLock.lock();
    try {
      task = super.peek();
    } finally {
      queueLock.unlock();
    }
    return task;
  }

  @Override
  public void remove() {
    queueLock.lock();
    try {
      super.remove();
    } finally {
      queueLock.unlock();
    }
  }


}

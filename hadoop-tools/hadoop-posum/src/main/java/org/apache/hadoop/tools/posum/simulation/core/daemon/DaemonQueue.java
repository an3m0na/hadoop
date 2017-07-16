package org.apache.hadoop.tools.posum.simulation.core.daemon;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DaemonQueue extends DelayQueue<Daemon> {
  private final Set<Daemon> running = Collections.synchronizedSet(new HashSet<Daemon>());
  private final Lock lock = new ReentrantLock();
  private final Condition notEmpty = lock.newCondition();
  private final Condition empty = lock.newCondition();
  private final Set<Daemon> untracked = Collections.synchronizedSet(new HashSet<Daemon>());

  void markUntracked(Daemon daemon) {
    untracked.add(daemon);
  }

  @Override
  public Daemon take() throws InterruptedException {
    lock.lock();
    try {
      Daemon element;
      while ((element = super.poll(0, TimeUnit.MILLISECONDS)) == null) {
        notEmpty.await();
      }
      updateState(element, true);
      return element;
    } finally {
      lock.unlock();
    }
  }

  void enqueue(Daemon element) {
    lock.lock();
    try {
      updateState(element, false);
      add(element);
      notEmpty.signalAll();
    } finally {
      lock.unlock();
    }
  }

  private void updateState(Daemon element, boolean isRunning) {
    if (untracked.contains(element))
      return;
    if (isRunning) {
      running.add(element);
    } else {
      running.remove(element);
      if (running.isEmpty())
        empty.signalAll();
    }
  }

  void evict(Daemon daemon) {
    lock.lock();
    try {
      updateState(daemon, false);
    } finally {
      lock.unlock();
    }
  }

  void awaitEmpty() throws InterruptedException {
    lock.lock();
    try {
      while (!running.isEmpty())
        empty.await();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public String toString() {
    return "DaemonQueue{" +
      "running=" + running +
      ", contents=" + super.toString() +
      '}';
  }
}

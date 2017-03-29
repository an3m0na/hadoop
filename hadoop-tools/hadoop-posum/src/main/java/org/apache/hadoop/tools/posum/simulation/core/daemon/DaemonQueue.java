package org.apache.hadoop.tools.posum.simulation.core.daemon;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

public class DaemonQueue extends DelayQueue<Daemon> {
  private Set<Daemon> running = Collections.newSetFromMap(new ConcurrentHashMap<Daemon, Boolean>());

  @Override
  public Daemon poll() {
    Daemon element = super.poll();
    running.add(element);
    return element;
  }

  @Override
  public Daemon take() throws InterruptedException {
    Daemon element = super.take();
    running.add(element);
    return element;
  }

  @Override
  public Daemon poll(long timeout, TimeUnit unit) throws InterruptedException {
    Daemon element = super.poll(timeout, unit);
    running.add(element);
    return element;
  }

  public void enqueue(Daemon element) {
    running.remove(element);
    add(element);
    synchronized (this) {
      notify();
    }
  }

  public void evict(Daemon element) {
    running.remove(element);
    synchronized (this) {
      notify();
    }
  }

  public int countRunning() {
    return running.size();
  }
}

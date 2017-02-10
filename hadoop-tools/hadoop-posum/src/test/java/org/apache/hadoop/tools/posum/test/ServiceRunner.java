package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.PosumMasterProcess;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ServiceRunner<T extends PosumMasterProcess> extends Thread {

  private T service = null;
  private Class<T> serviceClass;
  private Lock lock = new ReentrantLock();
  private final Condition isAvailable = lock.newCondition();
  private boolean available = false;

  public ServiceRunner(Class<T> serviceClass) {
    this.serviceClass = serviceClass;
  }

  @Override
  public void run() {
    lock.lock();
    Configuration conf = PosumConfiguration.newInstance();
    try {
      service = serviceClass.newInstance();
      service.init(conf);
      service.start();
      available = true;
      isAvailable.signal();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new PosumException("Could not instantiate service " + serviceClass, e);
    } finally {
      lock.unlock();
    }
  }

  public void awaitAvailability() throws InterruptedException {
    lock.lock();
    while (!available)
      isAvailable.await();
    lock.unlock();
  }

  public void shutDown() {
    service.stop();
  }

  public T getService() {
    return service;
  }
}

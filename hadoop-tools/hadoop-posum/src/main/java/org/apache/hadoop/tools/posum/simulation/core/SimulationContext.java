package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.util.communication.DatabaseProvider;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.cluster.TopologyProvider;
import org.apache.hadoop.tools.posum.simulation.core.daemon.DaemonQueue;
import org.apache.hadoop.tools.posum.simulation.core.dispatcher.SimpleDispatcher;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import java.util.concurrent.CountDownLatch;

public class SimulationContext implements DatabaseProvider {

  private volatile long currentTime = 0;
  private CountDownLatch remainingJobsCounter;
  private DaemonQueue daemonQueue;
  private Configuration conf = PosumConfiguration.newInstance();
  private Class<? extends ResourceScheduler> schedulerClass;
  private long startTime = 0;
  private long endTime = 0;
  private TopologyProvider topologyProvider;
  private Database database;
  private Database sourceDatabase;
  private Dispatcher dispatcher = new SimpleDispatcher();
  private JobBehaviorPredictor predictor;
  private boolean awaitingScheduler;

  public long getCurrentTime() {
    return currentTime;
  }

  public void setCurrentTime(long currentTime) {
    this.currentTime = currentTime;
  }

  public CountDownLatch getRemainingJobsCounter() {
    return remainingJobsCounter;
  }

  public void setRemainingJobsCounter(CountDownLatch remainingJobsCounter) {
    this.remainingJobsCounter = remainingJobsCounter;
  }

  public TopologyProvider getTopologyProvider() {
    return topologyProvider;
  }

  public void setTopologyProvider(TopologyProvider topologyProvider) {
    this.topologyProvider = topologyProvider;
  }

  public DaemonQueue getDaemonQueue() {
    return daemonQueue;
  }

  public void setDaemonQueue(DaemonQueue daemonQueue) {
    this.daemonQueue = daemonQueue;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Class<? extends ResourceScheduler> getSchedulerClass() {
    return schedulerClass;
  }

  public void setSchedulerClass(Class<? extends ResourceScheduler> schedulerClass) {
    this.schedulerClass = schedulerClass;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public void setDatabase(Database database) {
    this.database = database;
  }

  public Database getDatabase() {
    return database;
  }

  public Dispatcher getDispatcher() {
    return dispatcher;
  }

  public void setDispatcher(Dispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  public Database getSourceDatabase() {
    return sourceDatabase;
  }

  public void setSourceDatabase(Database sourceDatabase) {
    this.sourceDatabase = sourceDatabase;
  }

  public JobBehaviorPredictor getPredictor() {
    return predictor;
  }

  public void setPredictor(JobBehaviorPredictor predictor) {
    this.predictor = predictor;
  }

  public void setAwaitingScheduler(boolean awaitingScheduler) {
    this.awaitingScheduler = awaitingScheduler;
  }

  public boolean isAwaitingScheduler() {
    return awaitingScheduler;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  @Override
  public String toString() {
    return "SimulationContext{" +
      "currentTime=" + currentTime +
      ", remainingJobsCounter=" + remainingJobsCounter +
      ", daemonQueue=" + daemonQueue +
      ", conf=" + conf +
      ", schedulerClass=" + schedulerClass +
      ", startTime=" + startTime +
      ", endTime=" + endTime +
      ", topologyProvider=" + topologyProvider +
      ", database=" + database +
      ", sourceDatabase=" + sourceDatabase +
      ", dispatcher=" + dispatcher +
      ", predictor=" + predictor +
      ", awaitingScheduler=" + awaitingScheduler +
      '}';
  }
}

package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.TopologyProvider;
import org.apache.hadoop.tools.posum.simulation.core.daemon.DaemonQueue;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.NMDaemon;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class SimulationContext {

  private volatile long currentTime = 0;
  private CountDownLatch remainingJobsCounter;
  private DaemonQueue daemonQueue = new DaemonQueue();
  private Configuration conf;
  private Class<? extends ResourceScheduler> schedulerClass;
  private List<JobProfile> jobs;
  private Map<String, List<TaskProfile>> tasks;
  private Map<NodeId, NMDaemon> nodeManagers;
  private JobCompletionHandler jobCompletionHandler;
  private long endTime = 0;
  private TopologyProvider topologyProvider;

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

  public List<JobProfile> getJobs() {
    return jobs;
  }

  public void setJobs(List<JobProfile> jobs) {
    this.jobs = jobs;
  }

  public Map<String, List<TaskProfile>> getTasks() {
    return tasks;
  }

  public void setTasks(Map<String, List<TaskProfile>> tasks) {
    this.tasks = tasks;
  }

  public Map<NodeId, NMDaemon> getNodeManagers() {
    return nodeManagers;
  }

  public void setNodeManagers(Map<NodeId, NMDaemon> nodeManagers) {
    this.nodeManagers = nodeManagers;
  }

  public JobCompletionHandler getJobCompletionHandler() {
    return jobCompletionHandler;
  }

  public void setJobCompletionHandler(JobCompletionHandler jobCompletionHandler) {
    this.jobCompletionHandler = jobCompletionHandler;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

}

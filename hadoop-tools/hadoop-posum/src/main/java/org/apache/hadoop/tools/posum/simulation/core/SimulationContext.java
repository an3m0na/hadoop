package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.core.daemon.DaemonQueue;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.NMSimulator;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class SimulationContext {

  private volatile long currentTime = 0;
  private CountDownLatch remainingJobsCounter;
  private DaemonQueue daemonQueue = new DaemonQueue();
  private Configuration conf;
  private String schedulerClass;
  private Map<String, String> topology; // node -> rack
  private List<JobProfile> jobs;
  private Map<String, List<TaskProfile>> tasks;
  private Map<NodeId, NMSimulator> nodeManagers;
  private JobCompletionHandler jobCompletionHandler;
  private long endTime = 0;

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

  public Map<String, String> getTopology() {
    return topology;
  }

  public void setTopology(Map<String, String> topology) {
    this.topology = topology;
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

  public String getSchedulerClass() {
    return schedulerClass;
  }

  public void setSchedulerClass(String schedulerClass) {
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

  public Map<NodeId, NMSimulator> getNodeManagers() {
    return nodeManagers;
  }

  public void setNodeManagers(Map<NodeId, NMSimulator> nodeManagers) {
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

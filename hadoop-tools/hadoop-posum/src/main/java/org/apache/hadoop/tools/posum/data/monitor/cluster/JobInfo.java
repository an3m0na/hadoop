package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;

import java.util.List;

class JobInfo {
  private JobProfile profile;
  private JobConfProxy conf;
  private CountersProxy jobCounters;
  private List<TaskProfile> taskStubs;

  JobInfo() {
  }

  JobInfo(JobProfile profile, JobConfProxy conf, List<TaskProfile> taskStubs) {
    this.profile = profile;
    this.conf = conf;
    this.taskStubs = taskStubs;
  }

  JobInfo(JobProfile profile, JobConfProxy conf, CountersProxy jobCounters) {
    this.profile = profile;
    this.conf = conf;
    this.jobCounters = jobCounters;
  }

  JobProfile getProfile() {
    return profile;
  }

  void setProfile(JobProfile profile) {
    this.profile = profile;
  }

  JobConfProxy getConf() {
    return conf;
  }

  void setConf(JobConfProxy conf) {
    this.conf = conf;
  }

  CountersProxy getJobCounters() {
    return jobCounters;
  }

  void setJobCounters(CountersProxy jobCounters) {
    this.jobCounters = jobCounters;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    JobInfo jobInfo = (JobInfo) o;

    if (profile != null ? !profile.equals(jobInfo.profile) : jobInfo.profile != null) return false;
    if (conf != null ? !conf.equals(jobInfo.conf) : jobInfo.conf != null) return false;
    return jobCounters != null ? jobCounters.equals(jobInfo.jobCounters) : jobInfo.jobCounters == null;

  }

  @Override
  public int hashCode() {
    int result = profile != null ? profile.hashCode() : 0;
    result = 31 * result + (conf != null ? conf.hashCode() : 0);
    result = 31 * result + (jobCounters != null ? jobCounters.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "JobInfo{" +
      "profile=" + profile +
      ", conf=" + conf +
      ", jobCounters=" + jobCounters +
      '}';
  }

  public List<TaskProfile> getTaskStubs() {
    return taskStubs;
  }
}

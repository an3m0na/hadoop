package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;

class JobInfo {
    private JobProfile profile;
    private JobConfProxy conf;
    private CountersProxy jobCounters;

    JobInfo() {
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
}

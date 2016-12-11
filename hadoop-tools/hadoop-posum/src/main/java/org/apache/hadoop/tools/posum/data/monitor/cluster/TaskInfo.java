package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;

import java.util.List;

class TaskInfo {
    private List<TaskProfile> tasks;
    private List<CountersProxy> counters;

    TaskInfo(List<TaskProfile> tasks, List<CountersProxy> counters) {
        this.tasks = tasks;
        this.counters = counters;
    }

    List<TaskProfile> getTasks() {
        return tasks;
    }

    void setTasks(List<TaskProfile> tasks) {
        this.tasks = tasks;
    }

    List<CountersProxy> getCounters() {
        return counters;
    }

    void setCounters(List<CountersProxy> counters) {
        this.counters = counters;
    }
}

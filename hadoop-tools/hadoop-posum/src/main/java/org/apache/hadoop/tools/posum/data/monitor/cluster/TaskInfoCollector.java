package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.StoreCall;
import org.apache.hadoop.tools.posum.common.records.call.UpdateOrStoreCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.HistoryProfilePBImpl;
import org.apache.hadoop.tools.posum.common.util.Utils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;

public class TaskInfoCollector {

    private HadoopAPIClient api;
    private Database db;

    TaskInfoCollector(HadoopAPIClient api, Database db) {
        this.api = api;
        this.db = db;
    }

    List<TaskProfile> getFinishedTaskInfo(JobProfile job) {
        List<TaskProfile> tasks = api.getFinishedTasksInfo(job.getId());
        for (TaskProfile task : tasks) {
            api.addFinishedAttemptInfo(task);
            task.setAppId(job.getAppId());
            if (job.getSplitLocations() != null && task.getHttpAddress() != null) {
                int splitIndex = Utils.parseTaskId(task.getId()).getId();
                if (job.getSplitLocations().get(splitIndex).equals(task.getHttpAddress()))
                    task.setLocal(true);
            }
        }
        return tasks;
    }

    List<CountersProxy> updateFinishedTasksFromCounters(List<TaskProfile> tasks) {
        List<CountersProxy> countersList = new ArrayList<>(tasks.size());
        for (TaskProfile task : tasks) {
            CountersProxy counters = api.getFinishedTaskCounters(task.getJobId(), task.getId());
            if (counters != null) {
                Utils.updateTaskStatisticsFromCounters(task, counters);
                countersList.add(counters);
            }
        }
        return countersList;
    }

    List<TaskProfile> getRunningTaskInfo(JobProfile job) {
        List<TaskProfile> tasks = api.getRunningTasksInfo(job);
        if (tasks == null) {
            return null;
        }
        for (TaskProfile task : tasks) {
            task.setAppId(job.getAppId());
            if (!api.addRunningAttemptInfo(task)) {
                return null;
            }
        }
        return tasks;
    }

    List<CountersProxy> updateRunningTasksFromCounters(List<TaskProfile> tasks) {
        List<CountersProxy> countersList = new ArrayList<>(tasks.size());
        for (TaskProfile task : tasks) {
            CountersProxy counters = api.getRunningTaskCounters(task.getAppId(), task.getJobId(), task.getId());
            if (counters == null) {
                return null;
            }
            Utils.updateTaskStatisticsFromCounters(task, counters);
            countersList.add(counters);
        }
        return countersList;
    }
}

package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.tools.posum.common.records.JobProfile;
import org.apache.hadoop.tools.posum.common.records.TaskProfile;

import java.util.List;

/**
 * Created by ane on 2/9/16.
 */
public interface DataStore {


    TaskProfile getTaskProfile(TaskId taskId);

    JobProfile getJobProfile(JobId jobId);

    List<JobProfile> getComparableProfiles(String user, int count);

}

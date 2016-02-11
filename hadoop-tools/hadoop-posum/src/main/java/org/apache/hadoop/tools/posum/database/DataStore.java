package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.tools.posum.database.records.JobProfile;
import org.apache.hadoop.tools.posum.database.records.TaskProfile;

import java.util.List;

/**
 * Created by ane on 2/9/16.
 */
public interface DataStore {


    TaskProfile getTaskProfile(TaskID taskId);

    JobProfile getJobProfile(JobID jobId);

    List<JobProfile> getComparableProfiles(String user, int count);

}

package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.tools.posum.predictor.JobProfile;
import org.apache.hadoop.tools.posum.predictor.TaskProfile;

import java.util.List;

/**
 * Created by ane on 2/9/16.
 */
public interface DataStore {


    Float getAverageJobDuration(String user, String jobName);

    TaskProfile getTaskProfile(String taskId);

    JobProfile getJobProfile(String jobId);

    List<JobProfile> getComparableProfiles(String user, int count);

}

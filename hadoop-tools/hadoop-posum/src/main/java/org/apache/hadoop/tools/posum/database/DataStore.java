package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.tools.posum.database.records.JobProfile;
import org.apache.hadoop.tools.posum.database.records.TaskProfile;

import java.util.List;

/**
 * Created by ane on 2/9/16.
 */
public interface DataStore {


    TaskProfile getTaskProfile(String taskId);

    JobProfile getJobProfile(String jobId);

    List<JobProfile> getComparableProfiles(String user, int count);

}

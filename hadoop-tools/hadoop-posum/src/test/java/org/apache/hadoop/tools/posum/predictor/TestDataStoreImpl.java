package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.database.store.DataStoreInterface;
import org.apache.hadoop.tools.posum.database.store.DataStoreImpl;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by ane on 3/4/16.
 */
public class TestDataStoreImpl {
    @Test
    public void checkDatabase() {
        DataStoreInterface dataStoreInterface = new DataStoreImpl(TestUtils.getConf());
        dataStoreInterface.delete(DataEntityType.APP, "blabla1");
        dataStoreInterface.delete(DataEntityType.APP, "blabla2");
        AppProfile profile = Records.newRecord(AppProfile.class);
        profile.setId("blabla1");
        dataStoreInterface.store(DataEntityType.APP, profile);
        profile.setFinishTime(System.currentTimeMillis());
        dataStoreInterface.updateOrStore(DataEntityType.APP, profile);
        profile.setId("blabla2");
        assertTrue(dataStoreInterface.updateOrStore(DataEntityType.APP, profile));
        dataStoreInterface.delete(DataEntityType.APP, "blabla1");
        dataStoreInterface.delete(DataEntityType.APP, "blabla2");
    }

    @Test
    public void checkFinder() {
        DataStoreInterface dataStoreInterface = new DataStoreImpl(TestUtils.getConf());
        long time = System.currentTimeMillis();
        JobProfile job = Records.newRecord(JobProfile.class);
        job.setId("job_" + time + "_0000");
        job.setAppId("application_" + time + "_0000");
        job.setStartTime(time);
        job.setFinishTime(0L);
        job.setName("test job");
        job.setUser("THE user");
        job.setState(JobState.RUNNING);
        job.setMapProgress(0.44f);
        job.setReduceProgress(0.12f);
        job.setCompletedMaps(7);
        job.setCompletedReduces(1);
        job.setTotalMapTasks(15);
        job.setTotalReduceTasks(10);
        job.setUberized(false);
        try {
            dataStoreInterface.store(DataEntityType.JOB, job);
            assertEquals(job, dataStoreInterface.findById(DataEntityType.JOB, job.getId()));
        } finally {
            dataStoreInterface.delete(DataEntityType.JOB, job.getId());
        }
    }
}

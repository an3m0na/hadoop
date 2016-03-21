package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.tools.posum.database.store.DataStoreImpl;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by ane on 3/4/16.
 */
public class TestDataStoreImpl {
    @Test
    public void checkDatabase() {
        DataStore dataStore = new DataStoreImpl(TestUtils.getConf());
        dataStore.delete(DataEntityType.APP, "blabla1");
        dataStore.delete(DataEntityType.APP, "blabla2");
        AppProfile profile = new AppProfile("blabla1");
        dataStore.store(DataEntityType.APP, profile);
        profile.setFinishTime(System.currentTimeMillis());
        dataStore.updateOrStore(DataEntityType.APP, profile);
        profile.setId("blabla2");
        assertTrue(dataStore.updateOrStore(DataEntityType.APP, profile));
        dataStore.delete(DataEntityType.APP, "blabla1");
        dataStore.delete(DataEntityType.APP, "blabla2");
    }

    @Test
    public void checkFinder() {
        DataStore dataStore = new DataStoreImpl(TestUtils.getConf());
        long time = System.currentTimeMillis();
        JobProfile job = new JobProfile("job_" + time + "_0000");
        job.setAppId("application_" + time + "_0000");
        job.setStartTime(time);
        job.setFinishTime(0L);
        job.setName("test job");
        job.setUser("THE user");
        job.setState(JobState.RUNNING.toString());
        job.setMapProgress(0.44f);
        job.setReduceProgress(0.12f);
        job.setCompletedMaps(7);
        job.setCompletedReduces(1);
        job.setTotalMapTasks(15);
        job.setTotalReduceTasks(10);
        job.setUberized(false);
        try {
            dataStore.store(DataEntityType.JOB, job);
            assertEquals(job, dataStore.findById(DataEntityType.JOB, job.getId()));
        } finally {
            dataStore.delete(DataEntityType.JOB, job.getId());
        }
    }
}

package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.tools.posum.common.records.profile.AppProfile;
import org.apache.hadoop.tools.posum.common.records.profile.JobProfile;
import org.apache.hadoop.tools.posum.database.store.DataCollection;
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
        dataStore.delete(DataCollection.APPS, "blabla1");
        dataStore.delete(DataCollection.APPS, "blabla2");
        AppProfile profile = new AppProfile("blabla1");
        dataStore.store(DataCollection.APPS, profile);
        profile.setFinishTime(System.currentTimeMillis());
        dataStore.updateOrStore(DataCollection.APPS, profile);
        profile.setId("blabla2");
        assertTrue(dataStore.updateOrStore(DataCollection.APPS, profile));
        dataStore.delete(DataCollection.APPS, "blabla1");
        dataStore.delete(DataCollection.APPS, "blabla2");
    }

    @Test
    public void checkFinder() {
        DataStore dataStore = new DataStoreImpl(TestUtils.getConf());
        long time = System.currentTimeMillis();
        JobProfile job = new JobProfile("job_" + time + "_0000");
        job.setAppId("application_" + time + "_0000");
        job.setStartTime(time);
        job.setFinishTime(0L);
        job.setJobName("test job");
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
            dataStore.store(DataCollection.JOBS, job);
            assertEquals(job, dataStore.findById(DataCollection.JOBS, job.getId()));
        } finally {
            dataStore.delete(DataCollection.JOBS, job.getId());
        }
    }
}

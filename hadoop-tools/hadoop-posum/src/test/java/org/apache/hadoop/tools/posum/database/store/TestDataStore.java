package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by ane on 3/4/16.
 */
public class TestDataStore {

    private static final DataEntityDB db = DataEntityDB.getMain();

    @Test
    public void checkDatabase() {
        DataStore dataStore = new DataStore(POSUMConfiguration.newInstance());
        dataStore.delete(db, DataEntityCollection.APP, "blabla1");
        dataStore.delete(db, DataEntityCollection.APP, "blabla2");
        AppProfile profile = Records.newRecord(AppProfile.class);
        profile.setId("blabla1");
        dataStore.store(db, DataEntityCollection.APP, profile);
        profile.setFinishTime(System.currentTimeMillis());
        dataStore.updateOrStore(db, DataEntityCollection.APP, profile);
        profile.setId("blabla2");
        assertTrue(dataStore.updateOrStore(db, DataEntityCollection.APP, profile));
        dataStore.delete(db, DataEntityCollection.APP, "blabla1");
        dataStore.delete(db, DataEntityCollection.APP, "blabla2");
    }

    @Test
    public void checkFinder() {
        DataStore dataStore = new DataStore(POSUMConfiguration.newInstance());
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
            dataStore.store(db, DataEntityCollection.JOB, job);
            assertEquals(job, dataStore.findById(db, DataEntityCollection.JOB, job.getId()));
        } finally {
            dataStore.delete(db, DataEntityCollection.JOB, job.getId());
        }
    }
}

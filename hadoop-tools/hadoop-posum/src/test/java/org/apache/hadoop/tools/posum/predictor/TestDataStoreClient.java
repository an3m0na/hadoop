package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.database.client.DataStoreClient;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.tools.posum.database.store.DataStoreImpl;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by ane on 3/21/16.
 */
public class TestDataStoreClient {

    @Test
    public void checkOneObject() {
        Configuration conf = TestUtils.getConf();
        DataStoreClient dataStore = new DataStoreClient();
        dataStore.init(conf);
        dataStore.start();
        DataStore myStore = new DataStoreImpl(conf);

        String appId = "blabla1";
        myStore.delete(DataEntityType.APP, appId);
        AppProfile app = new AppProfile(appId);
        System.out.println(app);
        app.setStartTime(System.currentTimeMillis());
        app.setFinishTime(System.currentTimeMillis() + 10000);
        myStore.updateOrStore(DataEntityType.APP, app);

        AppProfile otherApp = dataStore.findById(DataEntityType.APP, appId);
        otherApp.setName("baghipeh app");
        myStore.updateOrStore(DataEntityType.APP, otherApp);
        System.out.println(otherApp);
        assertTrue(app.getId().equals(otherApp.getId()));

        String jobId = "blabla1_job1";
        myStore.delete(DataEntityType.JOB, jobId);
        JobProfile job = new JobProfile(jobId);
        System.out.println(job);
        job.setAppId(appId);
        job.setStartTime(System.currentTimeMillis());
        job.setFinishTime(System.currentTimeMillis() + 10000);
        myStore.updateOrStore(DataEntityType.JOB, job);

        JobProfile otherJob = dataStore.findById(DataEntityType.JOB, jobId);
        otherJob.setName("mvkh job");
        myStore.updateOrStore(DataEntityType.JOB, otherJob);
        System.out.println(otherJob);
        assertTrue(job.getId().equals(otherJob.getId()));

        myStore.delete(DataEntityType.JOB, jobId);
        myStore.delete(DataEntityType.APP, appId);
    }
}

package org.apache.hadoop.tools.posum.database.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.HistoryProfilePBImpl;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by ane on 3/21/16.
 */
public class TestDataMasterClient {

    private static final DataEntityDB db = DataEntityDB.getMain();

    @Test
    public void checkOneObject() {
        Configuration conf = POSUMConfiguration.newInstance();
        DataMasterClient dataStore = new DataMasterClient(null);
        dataStore.init(conf);
        dataStore.start();
        DataStore myStore = new DataStore(conf);

        String appId = "testApp";
        myStore.delete(db, DataEntityCollection.APP, appId);
        AppProfile app = Records.newRecord(AppProfile.class);
        app.setId(appId);
        app.setStartTime(System.currentTimeMillis());
        app.setFinishTime(System.currentTimeMillis() + 10000);
        System.out.println(app);
        myStore.updateOrStore(db, DataEntityCollection.APP, app);

        AppProfile otherApp = dataStore.findById(db, DataEntityCollection.APP, appId);
        otherApp.setName("Official Test App");
        myStore.updateOrStore(db, DataEntityCollection.APP, otherApp);
        System.out.println(otherApp);
        assertTrue(app.getId().equals(otherApp.getId()));

        String jobId = "testApp_job1";
        myStore.delete(db, DataEntityCollection.JOB, jobId);
        JobProfile job = Records.newRecord(JobProfile.class);
        job.setId(jobId);
        job.setAppId(appId);
        job.setStartTime(System.currentTimeMillis());
        job.setFinishTime(System.currentTimeMillis() + 10000);
        System.out.println(job);
        myStore.updateOrStore(db, DataEntityCollection.JOB, job);

        JobProfile otherJob = dataStore.findById(db, DataEntityCollection.JOB, jobId);
        otherJob.setName("Official Test Job");
        myStore.updateOrStore(db, DataEntityCollection.JOB, otherJob);
        System.out.println(otherJob);
        assertTrue(job.getId().equals(otherJob.getId()));


        String taskId = "testApp_job1_task1";
        myStore.delete(db, DataEntityCollection.TASK, taskId);
        TaskProfile task = Records.newRecord(TaskProfile.class);
        task.setId(taskId);
        task.setAppId(appId);
        task.setJobId(jobId);
        task.setStartTime(System.currentTimeMillis());
        task.setFinishTime(System.currentTimeMillis() + 10000);
        System.out.println(task);
        myStore.updateOrStore(db, DataEntityCollection.TASK, task);

        TaskProfile otherTask = dataStore.findById(db, DataEntityCollection.TASK, taskId);
        otherTask.setType("REDUCE");
        myStore.updateOrStore(db, DataEntityCollection.TASK, otherTask);
        System.out.println(otherTask);
        assertTrue(task.getId().equals(otherTask.getId()));

        myStore.delete(db, DataEntityCollection.TASK, taskId);
        myStore.delete(db, DataEntityCollection.JOB, jobId);
        myStore.delete(db, DataEntityCollection.APP, appId);
    }

    @Test
    public void checkMuliObject() {
        Configuration conf = POSUMConfiguration.newInstance();
        DataMasterClient dataStore = new DataMasterClient(null);
        dataStore.init(conf);
        dataStore.start();
        DataStore myStore = new DataStore(conf);

        String appId = "testHistoryApp";
        myStore.delete(db, DataEntityCollection.HISTORY, Collections.singletonMap("originalId", (Object)appId));
        AppProfile app = Records.newRecord(AppProfile.class);
        app.setId(appId);
        app.setStartTime(System.currentTimeMillis());
        app.setFinishTime(System.currentTimeMillis() + 10000);
        System.out.println(app);
        HistoryProfile appHistory = new HistoryProfilePBImpl(DataEntityCollection.APP, app);
        String historyId = myStore.store(db, DataEntityCollection.HISTORY, appHistory);

        Map<String, Object> properties = new HashMap<>();
        properties.put("originalId", appId);
        List<HistoryProfile> profilesById = dataStore.find(db, DataEntityCollection.HISTORY, properties, 0, 0);
        System.out.println(profilesById);
        assertTrue(profilesById.size() == 1);
        HistoryProfile otherHistory = profilesById.get(0);
        assertEquals(appId, otherHistory.getOriginalId());
        assertEquals(appHistory.getTimestamp(), otherHistory.getTimestamp());

        myStore.delete(db, DataEntityCollection.HISTORY, historyId);
    }
}

package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 7/29/16.
 */
public class Utils {
    public static final Long DURATION_UNIT = 60000L; // 1 minute
    public static final String JOB_NAME_ROOT = "Dummy Job";
    public static final String FIRST_USER = "dummy";
    public static final String SECOND_USER = "geek";

    public static void loadThreeDefaultAppsAndJobs(Long clusterTimestamp, DBInterface db){
        AppProfile app1 = Records.newRecord(AppProfile.class);
        ApplicationId app1Id = ApplicationId.newInstance(clusterTimestamp, 1);
        app1.setId(app1Id.toString());
        app1.setName(JOB_NAME_ROOT + " 1");
        app1.setUser(FIRST_USER);
        app1.setStartTime(clusterTimestamp - 5 * DURATION_UNIT);
        app1.setFinishTime(clusterTimestamp);
        db.store(DataEntityCollection.APP, app1);

        JobProfile job1 = Records.newRecord(JobProfile.class);
        JobId job1Id = new JobIdPBImpl();
        job1Id.setAppId(app1Id);
        job1Id.setId(1);
        job1.setId(job1Id.toString());
        job1.setAppId(app1.getId());
        job1.setName(app1.getName());
        job1.setUser(app1.getUser());
        job1.setTotalMapTasks(5);
        job1.setTotalReduceTasks(1);
        job1.setStartTime(app1.getStartTime());
        job1.setFinishTime(app1.getFinishTime());
        db.store(DataEntityCollection.JOB, job1);


        AppProfile app2 = Records.newRecord(AppProfile.class);
        ApplicationId app2Id = ApplicationId.newInstance(clusterTimestamp, 2);
        app2.setId(app2Id.toString());
        app2.setName(JOB_NAME_ROOT + " 2");
        app2.setUser(SECOND_USER);
        app2.setStartTime(clusterTimestamp - 4 * DURATION_UNIT);
        app2.setFinishTime(clusterTimestamp - DURATION_UNIT);
        db.store(DataEntityCollection.APP, app2);

        JobProfile job2 = Records.newRecord(JobProfile.class);
        JobId job2Id = new JobIdPBImpl();
        job2Id.setAppId(app2Id);
        job2Id.setId(2);
        job2.setId(job2Id.toString());
        job2.setAppId(app2.getId());
        job2.setName(app2.getName());
        job2.setUser(app2.getUser());
        job2.setTotalMapTasks(10);
        job2.setTotalReduceTasks(3);
        job2.setStartTime(app2.getStartTime());
        job2.setFinishTime(app2.getFinishTime());
        db.store(DataEntityCollection.JOB, job2);

        AppProfile app3 = Records.newRecord(AppProfile.class);
        ApplicationId app3Id = ApplicationId.newInstance(clusterTimestamp, 3);
        app3.setId(app3Id.toString());
        app3.setName(JOB_NAME_ROOT + " 3");
        app3.setUser(SECOND_USER);
        app3.setStartTime(clusterTimestamp - 2 * DURATION_UNIT);
        app3.setFinishTime(clusterTimestamp - DURATION_UNIT);
        db.store(DataEntityCollection.APP, app3);

        JobProfile job3 = Records.newRecord(JobProfile.class);
        JobId job3Id = new JobIdPBImpl();
        job3Id.setAppId(app3Id);
        job3Id.setId(3);
        job3.setId(job3Id.toString());
        job3.setAppId(app3.getId());
        job3.setName(app3.getName());
        job3.setUser(app3.getUser());
        job3.setTotalMapTasks(1);
        job3.setTotalReduceTasks(1);
        job3.setStartTime(app3.getStartTime());
        job3.setFinishTime(app3.getFinishTime());
        db.store(DataEntityCollection.JOB, job3);
    }
}
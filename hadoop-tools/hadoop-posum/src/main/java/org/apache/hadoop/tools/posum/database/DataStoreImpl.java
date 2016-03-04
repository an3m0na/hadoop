package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.tools.posum.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.records.AppProfile;
import org.apache.hadoop.tools.posum.common.records.GeneralProfile;
import org.apache.hadoop.tools.posum.common.records.JobProfile;
import org.apache.hadoop.tools.posum.common.records.TaskProfile;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public class DataStoreImpl implements DataStore, Configurable {

    private Configuration conf = new Configuration(false);
    MongoJackConnector conn;

    public DataStoreImpl(Configuration conf) {
        setConf(conf);
        String name = conf.get(POSUMConfiguration.DATABASE_NAME, POSUMConfiguration.DATABASE_NAME_DEFAULT);
        String url = conf.get(POSUMConfiguration.DATABASE_URL, POSUMConfiguration.DATABASE_URL_DEFAULT);
        conn = new MongoJackConnector(name, url);
        for (DataCollection collection : DataCollection.values()) {
            conn.addCollection(collection);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public TaskProfile getTaskProfile(TaskId taskId) {
        return conn.findObjectById(DataCollection.TASKS, taskId.toString());
    }

    @Override
    public JobProfile getJobProfile(String jobId) {
        return conn.findObjectById(DataCollection.JOBS, jobId.toString());
    }

    @Override
    public AppProfile getAppProfile(ApplicationId appId) {
        return conn.findObjectById(DataCollection.APPS, appId.toString());
    }

    @Override
    public <T extends GeneralProfile> void store(DataCollection collection, T toInsert) {
        conn.insertObject(collection, toInsert);
    }

    @Override
    public List<JobProfile> getComparableProfiles(String user, int count) {
        return null;
    }

    @Override
    public <T extends GeneralProfile> boolean updateOrStore(DataCollection collection, T toUpdate) {
        return conn.upsertObject(collection, toUpdate);
    }

    @Override
    public void delete(DataCollection collection, String id) {
        conn.deleteObject(collection, id);
    }

    @Override
    public void delete(DataCollection collection, String field, Object value) {
        conn.deleteObjects(collection, field, value);
    }

    @Override
    public void delete(DataCollection collection, Map<String, Object> queryParams) {
        conn.deleteObject(collection, queryParams);
    }


}

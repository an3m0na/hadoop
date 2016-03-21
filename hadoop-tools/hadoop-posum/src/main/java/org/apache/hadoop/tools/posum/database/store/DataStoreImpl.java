package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

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
        for (DataEntityType collection : DataEntityType.values()) {
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
    public <T extends GeneralDataEntity> T findById(DataEntityType collection, String id) {
        return conn.findObjectById(collection, id);
    }

    @Override
    public JobProfile getJobProfileForApp(String appId) {
        List<JobProfile> profiles = conn.findObjects(DataEntityType.JOB, "appId", appId);
        if (profiles.size() > 1)
            throw new YarnRuntimeException("Found too many profiles in database for app " + appId);
        if (profiles.size() < 1)
            return null;
        return profiles.get(0);
    }

    @Override
    public <T extends GeneralDataEntity> void store(DataEntityType collection, T toInsert) {
        conn.insertObject(collection, toInsert);
    }

    @Override
    public List<JobProfile> getComparableProfiles(String user, int count) {
        return null;
    }

    @Override
    public <T extends GeneralDataEntity> boolean updateOrStore(DataEntityType collection, T toUpdate) {
        return conn.upsertObject(collection, toUpdate);
    }

    @Override
    public void delete(DataEntityType collection, String id) {
        conn.deleteObject(collection, id);
    }

    @Override
    public void delete(DataEntityType collection, String field, Object value) {
        conn.deleteObjects(collection, field, value);
    }

    @Override
    public void delete(DataEntityType collection, Map<String, Object> queryParams) {
        conn.deleteObject(collection, queryParams);
    }


}

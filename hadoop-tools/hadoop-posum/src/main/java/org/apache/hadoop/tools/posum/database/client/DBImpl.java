package org.apache.hadoop.tools.posum.database.client;

import org.apache.hadoop.tools.posum.common.records.dataentity.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 5/17/16.
 */
public class DBImpl implements DBInterface {
    private final DataEntityDB db;
    private final DataMasterClient client;

    public DBImpl(DataEntityDB db, DataMasterClient client) {
        this.db = db;
        this.client = client;
    }

    @Override
    public <T extends GeneralDataEntity> List<T> list(DataEntityType collection) {
        return client.find(db, collection, new HashMap<String, Object>());
    }

    @Override
    public <T extends GeneralDataEntity> T findById(DataEntityType collection, String id) {
        return client.findById(db, collection, id);
    }

    @Override
    public <T extends GeneralDataEntity> List<T> find(DataEntityType collection, String field, Object value) {
        Map<String, Object> queryParams = new HashMap<>(1);
        queryParams.put(field, value);
        return client.find(db, collection, queryParams);
    }

    @Override
    public <T extends GeneralDataEntity> List<T> find(DataEntityType collection, Map<String, Object> queryParams) {
        return client.find(db, collection, queryParams);
    }

    @Override
    public <T extends GeneralDataEntity> List<T> find(DataEntityType collection, Map<String, Object> queryParams, int offset, int limit) {
        return client.find(db, collection, queryParams, offset, limit);
    }

    @Override
    public <T extends GeneralDataEntity> String store(DataEntityType collection, T toInsert) {
        return client.store(db, collection, toInsert);
    }

    @Override
    public <T extends GeneralDataEntity> boolean updateOrStore(DataEntityType apps, T toUpdate) {
        return client.updateOrStore(db, apps, toUpdate);
    }

    @Override
    public void delete(DataEntityType collection, String id) {
        client.delete(db, collection, id);
    }

    @Override
    public void delete(DataEntityType collection, String field, Object value) {
        Map<String, Object> queryParams = new HashMap<>(1);
        queryParams.put(field, value);
        client.delete(db, collection, queryParams);
    }

    @Override
    public void delete(DataEntityType collection, Map<String, Object> queryParams) {
        client.delete(db, collection, queryParams);
    }

    @Override
    public JobProfile getJobProfileForApp(String appId, String user) {
        return client.getJobProfileForApp(db, appId, user);
    }

    @Override
    public JobConfProxy getJobConf(String jobId) {
        return client.getJobConf(db, jobId);
    }

    @Override
    public void saveFlexFields(String jobId, Map<String, String> newFields) {
        client.saveFlexFields(db, jobId, newFields);
    }
}

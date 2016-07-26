package org.apache.hadoop.tools.posum.database.client;

import org.apache.hadoop.tools.posum.common.records.dataentity.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 5/17/16.
 */
public class DBImpl implements DBInterface {
    private final DataEntityDB db;
    private final DataClientInterface client;

    public DBImpl(DataEntityDB db, DataClientInterface client) {
        this.db = db;
        this.client = client;
    }

    @Override
    public <T extends GeneralDataEntity> List<T> list(DataEntityType collection) {
        return client.find(db, collection, null, 0, 0);
    }

    @Override
    public List<String> listIds(DataEntityType collection, Map<String, Object> queryParams) {
        return client.listIds(db, collection, queryParams);
    }

    @Override
    public <T extends GeneralDataEntity> T findById(DataEntityType collection, String id) {
        return client.findById(db, collection, id);
    }

    @Override
    public <T extends GeneralDataEntity> List<T> find(DataEntityType collection, String field, Object value) {
        return find(collection, field, value, 0, 0);
    }

    @Override
    public <T extends GeneralDataEntity> List<T> find(DataEntityType collection, String field, Object value, int offset, int limit) {
        return find(collection, Collections.singletonMap(field, value), offset, limit);
    }

    @Override
    public <T extends GeneralDataEntity> List<T> find(DataEntityType collection, Map<String, Object> queryParams) {
        return client.find(db, collection, queryParams, 0, 0);
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
    public <T extends GeneralDataEntity> boolean updateOrStore(DataEntityType collection, T toUpdate) {
        return client.updateOrStore(db, collection, toUpdate);
    }

    @Override
    public void delete(DataEntityType collection, String id) {
        client.delete(db, collection, id);
    }

    @Override
    public void delete(DataEntityType collection, String field, Object value) {
        client.delete(db, collection, Collections.singletonMap(field, value));
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
        return client.findById(db, DataEntityType.JOB_CONF, jobId);
    }

    @Override
    public void saveFlexFields(String jobId, Map<String, String> newFields, boolean forHistory) {
        client.saveFlexFields(db, jobId, newFields, forHistory);
    }
}

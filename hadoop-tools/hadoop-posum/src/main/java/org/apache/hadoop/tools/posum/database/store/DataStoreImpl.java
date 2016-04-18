package org.apache.hadoop.tools.posum.database.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.database.monitor.ClusterInfoCollector;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.mongojack.DBQuery;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ane on 2/9/16.
 */
public class DataStoreImpl implements DataStoreInterface {

    private static Log logger = LogFactory.getLog(DataStoreImpl.class);

    private final Configuration conf;
    private MongoJackConnector conn;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();

    public DataStoreImpl(Configuration conf) {
        String name = conf.get(POSUMConfiguration.DATABASE_NAME, POSUMConfiguration.DATABASE_NAME_DEFAULT);
        String url = conf.get(POSUMConfiguration.DATABASE_URL, POSUMConfiguration.DATABASE_URL_DEFAULT);
        conn = new MongoJackConnector(name, url);
        for (DataEntityType collection : DataEntityType.values()) {
            conn.addCollection(collection);
        }
        this.conf = conf;
    }

    @Override
    public <T extends GeneralDataEntity> T findById(DataEntityType collection, String id) {
        readLock.lock();
        try {
            return conn.findObjectById(collection, id);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public <T extends GeneralDataEntity> List<T> find(DataEntityType collection, String field, Object value) {
        readLock.lock();
        try {
            return conn.findObjects(collection, field, value);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public <T extends GeneralDataEntity> List<T> find(DataEntityType
                                                              collection, Map<String, Object> queryParams) {
        readLock.lock();
        try {
            return conn.findObjects(collection, queryParams);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public <T extends GeneralDataEntity> List<T> list(DataEntityType collection) {
        readLock.lock();
        try {
            return conn.findObjects(collection, (DBQuery.Query) null);
        } finally {
            readLock.unlock();

        }
    }

    @Override
    public JobProfile getJobProfileForApp(String appId) {
        readLock.lock();
        List<JobProfile> profiles;
        try {
            profiles = conn.findObjects(DataEntityType.JOB, "appId", appId);
        } finally {
            readLock.unlock();
        }
        if (profiles.size() == 1)
            return profiles.get(0);
        if (profiles.size() > 1)
            throw new YarnRuntimeException("Found too many profiles in database for app " + appId);

        //if not found, force the reading of the configuration
        try {
            return ClusterInfoCollector.getSubmittedJobInfo(conf, appId);
        } catch (Exception e) {
            logger.debug("Could not retrieve job info for app " + appId, e);
        }
        return null;

    }

    @Override
    public <T extends GeneralDataEntity> String store(DataEntityType collection, T toInsert) {
        writeLock.lock();
        try {
            return conn.insertObject(collection, toInsert);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public List<JobProfile> getComparableProfiles(String user, int count) {
        return null;
    }

    @Override
    public <T extends GeneralDataEntity> boolean updateOrStore(DataEntityType collection, T
            toUpdate) {
        writeLock.lock();
        try {
            return conn.upsertObject(collection, toUpdate);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void delete(DataEntityType collection, String id) {
        writeLock.lock();
        try {
            conn.deleteObject(collection, id);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void delete(DataEntityType collection, String field, Object value) {
        writeLock.lock();
        try {
            conn.deleteObjects(collection, field, value);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void delete(DataEntityType collection, Map<String, Object> queryParams) {
        writeLock.lock();
        try {
            conn.deleteObject(collection, queryParams);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void runTransaction(DataTransaction transaction) throws POSUMException {
        writeLock.lock();
        try {
            transaction.run();
        } catch (Exception e) {
            throw new POSUMException("Exception executing transaction ", e);
        } finally {
            writeLock.unlock();
        }
    }


}
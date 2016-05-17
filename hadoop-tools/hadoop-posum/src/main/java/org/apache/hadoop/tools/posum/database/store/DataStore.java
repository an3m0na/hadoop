package org.apache.hadoop.tools.posum.database.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
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
public class DataStore {

    private static Log logger = LogFactory.getLog(DataStore.class);

    private final Configuration conf;
    private MongoJackConnector conn;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();

    public DataStore(Configuration conf) {
        String url = conf.get(POSUMConfiguration.DATABASE_URL, POSUMConfiguration.DATABASE_URL_DEFAULT);
        conn = new MongoJackConnector(url);
        conn.addCollections(DataEntityDB.getMain(),
                DataEntityType.APP,
                DataEntityType.APP_HISTORY,
                DataEntityType.JOB,
                DataEntityType.JOB_HISTORY,
                DataEntityType.TASK,
                DataEntityType.TASK_HISTORY,
                DataEntityType.HISTORY);
        conn.addCollections(DataEntityDB.getSimulation(),
                DataEntityType.APP,
                DataEntityType.JOB,
                DataEntityType.TASK);
        this.conf = conf;
    }

    public <T extends GeneralDataEntity> T findById(DataEntityDB db, DataEntityType collection, String id) {
        readLock.lock();
        try {
            return conn.findObjectById(db, collection, id);
        } finally {
            readLock.unlock();
        }
    }

    public <T extends GeneralDataEntity> List<T> find(DataEntityDB db, DataEntityType collection, String field, Object value) {
        readLock.lock();
        try {
            return conn.findObjects(db, collection, field, value);
        } finally {
            readLock.unlock();
        }
    }

    public <T extends GeneralDataEntity> List<T> find(DataEntityDB db, DataEntityType
                                                              collection, Map<String, Object> queryParams) {
        readLock.lock();
        try {
            return conn.findObjects(db, collection, queryParams);
        } finally {
            readLock.unlock();
        }
    }

    public <T extends GeneralDataEntity> List<T> list(DataEntityDB db, DataEntityType collection) {
        readLock.lock();
        try {
            return conn.findObjects(db, collection, (DBQuery.Query) null);
        } finally {
            readLock.unlock();

        }
    }

    public JobProfile getJobProfileForApp(DataEntityDB db, String appId) {
        readLock.lock();
        List<JobProfile> profiles;
        try {
            profiles = conn.findObjects(db, DataEntityType.JOB, "appId", appId);
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

    public <T extends GeneralDataEntity> String store(DataEntityDB db, DataEntityType collection, T toInsert) {
        writeLock.lock();
        try {
            return conn.insertObject(db, collection, toInsert);
        } finally {
            writeLock.unlock();
        }
    }

    public List<JobProfile> getComparableProfiles(DataEntityDB db, String user, int count) {
        //TODO
        return null;
    }

    public <T extends GeneralDataEntity> boolean updateOrStore(DataEntityDB db, DataEntityType collection, T
            toUpdate) {
        writeLock.lock();
        try {
            return conn.upsertObject(db, collection, toUpdate);
        } finally {
            writeLock.unlock();
        }
    }

    public void delete(DataEntityDB db, DataEntityType collection, String id) {
        writeLock.lock();
        try {
            conn.deleteObject(db, collection, id);
        } finally {
            writeLock.unlock();
        }
    }

    public void delete(DataEntityDB db, DataEntityType collection, String field, Object value) {
        writeLock.lock();
        try {
            conn.deleteObjects(db, collection, field, value);
        } finally {
            writeLock.unlock();
        }
    }

    public void delete(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams) {
        writeLock.lock();
        try {
            conn.deleteObject(db, collection, queryParams);
        } finally {
            writeLock.unlock();
        }
    }

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

    public String getRawDocumentList(String database, String collection, Map<String, Object> queryParams) throws POSUMException {
        writeLock.lock();
        try {
            return conn.getRawDocumentList(database, collection, queryParams);
        } catch (Exception e) {
            throw new POSUMException("Exception executing transaction ", e);
        } finally {
            writeLock.unlock();
        }
    }

}

package org.apache.hadoop.tools.posum.database.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.database.monitor.ClusterInfoCollector;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.mongojack.DBQuery;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ane on 2/9/16.
 */
public class DataStore {

    private static Log logger = LogFactory.getLog(DataStore.class);

    private final Configuration conf;
    private MongoJackConnector conn;
    private ConcurrentHashMap<Integer, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();

    public DataStore(Configuration conf) {
        String url = conf.get(POSUMConfiguration.DATABASE_URL, POSUMConfiguration.DATABASE_URL_DEFAULT);
        conn = new MongoJackConnector(url);
        DataEntityDB db = DataEntityDB.getMain();
        conn.addCollections(db,
                DataEntityType.APP,
                DataEntityType.APP_HISTORY,
                DataEntityType.JOB,
                DataEntityType.JOB_HISTORY,
                DataEntityType.TASK,
                DataEntityType.TASK_HISTORY,
                DataEntityType.HISTORY);
        locks.put(db.getId(), new ReentrantReadWriteLock());
        conn.addCollections(DataEntityDB.getLogs(),
                DataEntityType.LOG_SCHEDULER);
        locks.put(db.getId(), new ReentrantReadWriteLock());
        conn.addCollections(DataEntityDB.getSimulation(),
                DataEntityType.APP,
                DataEntityType.JOB,
                DataEntityType.TASK);
        locks.put(db.getId(), new ReentrantReadWriteLock());
        this.conf = conf;
    }

    public <T extends GeneralDataEntity> T findById(DataEntityDB db, DataEntityType collection, String id) {
        locks.get(db.getId()).readLock().lock();
        try {
            return conn.findObjectById(db, collection, id);
        } finally {
            locks.get(db.getId()).readLock().unlock();
        }
    }

    public <T extends GeneralDataEntity> List<T> find(DataEntityDB db, DataEntityType collection, String field, Object value) {
        locks.get(db.getId()).readLock().lock();
        try {
            return conn.findObjects(db, collection, field, value);
        } finally {
            locks.get(db.getId()).readLock().unlock();
        }
    }

    public <T extends GeneralDataEntity> List<T> find(DataEntityDB db, DataEntityType
            collection, Map<String, Object> queryParams) {
        locks.get(db.getId()).readLock().lock();
        try {
            return conn.findObjects(db, collection, queryParams);
        } finally {
            locks.get(db.getId()).readLock().unlock();
        }
    }

    public <T extends GeneralDataEntity> List<T> list(DataEntityDB db, DataEntityType collection) {
        locks.get(db.getId()).readLock().lock();
        try {
            return conn.findObjects(db, collection, (DBQuery.Query) null);
        } finally {
            locks.get(db.getId()).readLock().unlock();

        }
    }

    public JobProfile getJobProfileForApp(DataEntityDB db, String appId) {
        locks.get(db.getId()).readLock().lock();
        List<JobProfile> profiles;
        try {
            profiles = conn.findObjects(db, DataEntityType.JOB, "appId", appId);
        } finally {
            locks.get(db.getId()).readLock().unlock();
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
        locks.get(db.getId()).writeLock().lock();
        try {
            return conn.insertObject(db, collection, toInsert);
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    public List<JobProfile> getComparableProfiles(DataEntityDB db, String user, int count) {
        //TODO
        return null;
    }

    public <T extends GeneralDataEntity> boolean updateOrStore(DataEntityDB db, DataEntityType collection, T
            toUpdate) {
        locks.get(db.getId()).writeLock().lock();
        try {
            return conn.upsertObject(db, collection, toUpdate);
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    public void delete(DataEntityDB db, DataEntityType collection, String id) {
        locks.get(db.getId()).writeLock().lock();
        try {
            conn.deleteObject(db, collection, id);
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    public void delete(DataEntityDB db, DataEntityType collection, String field, Object value) {
        locks.get(db.getId()).writeLock().lock();
        try {
            conn.deleteObjects(db, collection, field, value);
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    public void delete(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams) {
        locks.get(db.getId()).writeLock().lock();
        try {
            conn.deleteObject(db, collection, queryParams);
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    public void runTransaction(DataEntityDB db, DataTransaction transaction) throws POSUMException {
        locks.get(db.getId()).writeLock().lock();
        try {
            transaction.run();
        } catch (Exception e) {
            throw new POSUMException("Exception executing transaction ", e);
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    public String getRawDocumentList(String database, String collection, Map<String, Object> queryParams) throws POSUMException {
        try {
            return conn.getRawDocumentList(database, collection, queryParams);
        } catch (Exception e) {
            throw new POSUMException("Exception executing transaction ", e);
        }
    }

    public <T> void logAction(LogEntry<T> logEntry) {

    }
}

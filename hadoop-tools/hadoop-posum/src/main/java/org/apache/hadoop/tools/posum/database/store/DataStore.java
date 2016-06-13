package org.apache.hadoop.tools.posum.database.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.database.client.DBImpl;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.tools.posum.database.client.DataClientInterface;
import org.apache.hadoop.tools.posum.database.monitor.ClusterInfoCollector;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Records;
import org.mongojack.DBQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ane on 2/9/16.
 */
public class DataStore implements DataClientInterface {

    private static Log logger = LogFactory.getLog(DataStore.class);

    private final Configuration conf;
    private MongoJackConnector conn;
    private DataEntityDB mainDb = DataEntityDB.getMain(),
            logDb = DataEntityDB.getLogs(),
            simDb = DataEntityDB.getSimulation();
    private ConcurrentHashMap<Integer, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();

    public DataStore(Configuration conf) {
        String url = conf.get(POSUMConfiguration.DATABASE_URL, POSUMConfiguration.DATABASE_URL_DEFAULT);
        conn = new MongoJackConnector(url);
        conn.addDatabase(mainDb,
                DataEntityType.APP,
                DataEntityType.APP_HISTORY,
                DataEntityType.JOB,
                DataEntityType.JOB_HISTORY,
                DataEntityType.JOB_CONF,
                DataEntityType.JOB_CONF_HISTORY,
                DataEntityType.COUNTER,
                DataEntityType.COUNTER_HISTORY,
                DataEntityType.TASK,
                DataEntityType.TASK_HISTORY,
                DataEntityType.HISTORY);
        locks.put(mainDb.getId(), new ReentrantReadWriteLock());
        conn.dropDatabase(logDb);
        conn.addDatabase(logDb,
                DataEntityType.LOG_SCHEDULER,
                DataEntityType.LOG_PREDICTOR,
                DataEntityType.POSUM_STATS);
        locks.put(logDb.getId(), new ReentrantReadWriteLock());
        conn.addDatabase(simDb,
                DataEntityType.APP,
                DataEntityType.JOB,
                DataEntityType.TASK);
        locks.put(simDb.getId(), new ReentrantReadWriteLock());
        this.conf = conf;
    }

    @Override
    public <T extends GeneralDataEntity> T findById(DataEntityDB db, DataEntityType collection, String id) {
        locks.get(db.getId()).readLock().lock();
        try {
            return conn.findObjectById(db, collection, id);
        } finally {
            locks.get(db.getId()).readLock().unlock();
        }
    }

    @Override
    public <T extends GeneralDataEntity> List<T> find(DataEntityDB db,
                                                      DataEntityType collection,
                                                      Map<String, Object> queryParams,
                                                      int offset,
                                                      int limit) {
        locks.get(db.getId()).readLock().lock();
        try {
            return conn.findObjects(db, collection, queryParams, offset, limit);
        } finally {
            locks.get(db.getId()).readLock().unlock();
        }
    }

    @Override
    public List<String> listIds(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams) {
        locks.get(db.getId()).readLock().lock();
        List rawList = Collections.emptyList();
        try {
            rawList = conn.findObjects(db, collection, queryParams, "_id");
        } finally {
            locks.get(db.getId()).readLock().unlock();
        }
        List<String> ret = new ArrayList<>(rawList.size());
        for (Object rawObject : rawList) {
            ret.add(collection.getMappedClass().cast(rawObject).getId());
        }
        return ret;
    }

    @Override
    public JobProfile getJobProfileForApp(final DataEntityDB db, String appId, String user) {
        List<JobProfile> profiles;
        profiles = find(db, DataEntityType.JOB, Collections.singletonMap("appId", (Object) appId), 0, 0);
        if (profiles.size() == 1)
            return profiles.get(0);
        if (profiles.size() > 1)
            throw new YarnRuntimeException("Found too many profiles in database for app " + appId);

        // if not found, force the reading of the configuration
        // we need this because the information needs to be in the database for certain schedulers
        try {
            return ClusterInfoCollector.getAndStoreSubmittedJobInfo(conf, appId, user, this, db);
        } catch (Exception e) {
            logger.debug("Could not retrieve job info for app " + appId, e);
        }
        return null;
    }

    @Override
    public void saveFlexFields(final DataEntityDB db, String jobId, Map<String, String> newFields, boolean forHistory) {
        locks.get(db.getId()).writeLock().lock();
        try {
            DataEntityType type = forHistory ? DataEntityType.JOB_HISTORY : DataEntityType.JOB;
            JobProfile job = findById(db, type, jobId);
            if (job == null)
                throw new POSUMException("Could not find job to save flex-fields: " + jobId);

            job.getFlexFields().putAll(newFields);
            updateOrStore(db, type, job);
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    @Override
    public <T extends GeneralDataEntity> String store(DataEntityDB db, DataEntityType collection, T toInsert) {
        locks.get(db.getId()).writeLock().lock();
        try {
            return conn.insertObject(db, collection, toInsert);
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    @Override
    public <T extends GeneralDataEntity> boolean updateOrStore(DataEntityDB db, DataEntityType collection, T
            toUpdate) {
        locks.get(db.getId()).writeLock().lock();
        try {
            return conn.upsertObject(db, collection, toUpdate);
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    @Override
    public void delete(DataEntityDB db, DataEntityType collection, String id) {
        locks.get(db.getId()).writeLock().lock();
        try {
            conn.deleteObject(db, collection, id);
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    @Override
    public void delete(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams) {
        locks.get(db.getId()).writeLock().lock();
        try {
            conn.deleteObject(db, collection, queryParams);
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    @Override
    public DBInterface bindTo(DataEntityDB db) {
        return new DBImpl(db, this);
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

    public String getRawDocumentList(String database, String collection, Map<String, Object> queryParams)
            throws POSUMException {
        try {
            return conn.getRawDocumentList(database, collection, queryParams);
        } catch (Exception e) {
            throw new POSUMException("Exception executing transaction ", e);
        }
    }

    public <T> void storeLogEntry(LogEntry<T> logEntry) {
        updateOrStore(logDb, logEntry.getType().getCollection(), logEntry);
    }

    public <T> List<LogEntry<T>> findLogs(LogEntry.Type type, long from, long to) {
        locks.get(logDb.getId()).readLock().lock();
        try {
            return conn.findObjects(logDb, type.getCollection(), DBQuery.and(
                    DBQuery.greaterThan("timestamp", from),
                    DBQuery.lessThanEquals("timestamp", to),
                    DBQuery.is("type", type))
            );
        } finally {
            locks.get(logDb.getId()).readLock().unlock();
        }
    }

    public <T> List<LogEntry<T>> findLogs(LogEntry.Type type, long after) {
        locks.get(logDb.getId()).readLock().lock();
        try {
            return conn.findObjects(logDb, type.getCollection(), DBQuery.and(
                    DBQuery.greaterThan("timestamp", after),
                    DBQuery.is("type", type))
            );
        } finally {
            locks.get(logDb.getId()).readLock().unlock();
        }
    }

    public <T> LogEntry<T> findReport(LogEntry.Type type) {
        return findById(logDb, type.getCollection(), type.name());
    }

    public <T> void storeLogReport(LogEntry<T> logReport) {
        logReport.setId(logReport.getType().name());
        storeLogEntry(logReport);
    }
}

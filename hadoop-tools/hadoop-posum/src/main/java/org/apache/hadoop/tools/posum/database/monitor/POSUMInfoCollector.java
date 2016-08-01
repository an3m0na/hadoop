package org.apache.hadoop.tools.posum.database.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.payload.TaskPredictionPayload;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.PolicyMap;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.tools.posum.simulator.predictor.BasicPredictor;
import org.apache.hadoop.tools.posum.simulator.predictor.DetailedPredictor;
import org.apache.hadoop.tools.posum.simulator.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.simulator.predictor.StandardPredictor;

import java.util.List;

/**
 * Created by ane on 2/4/16.
 */
public class POSUMInfoCollector {

    private static Log logger = LogFactory.getLog(POSUMInfoCollector.class);

    private final POSUMAPIClient api;
    private final DataEntityDB db = DataEntityDB.getLogs();
    private final DataStore dataStore;
    private final Configuration conf;
    private final boolean fineGrained;
    private final PolicyMap policyMap;
    private long lastCollectTime = 0;
    private long lastPrediction = 0;
    private long predictionTimeout = 0;
    //TODO use only this predictor for regular experiments
    //    private JobBehaviorPredictor predictor;
    private JobBehaviorPredictor basicPredictor;
    private JobBehaviorPredictor standardPredictor;
    private JobBehaviorPredictor detailedPredictor;


    public POSUMInfoCollector(Configuration conf, DataStore dataStore) {
        this.dataStore = dataStore;
        this.conf = conf;
        fineGrained = conf.getBoolean(POSUMConfiguration.FINE_GRAINED_MONITOR,
                POSUMConfiguration.FINE_GRAINED_MONITOR_DEFAULT);
        api = new POSUMAPIClient(conf);
        this.policyMap = new PolicyMap(conf);
        DBInterface predictorDb = dataStore.bindTo(DataEntityDB.getMain());
//        predictor = JobBehaviorPredictor.newInstance(conf);
        basicPredictor = JobBehaviorPredictor.newInstance(conf, BasicPredictor.class);
        basicPredictor.initialize(predictorDb);
        standardPredictor = JobBehaviorPredictor.newInstance(conf, StandardPredictor.class);
        standardPredictor.initialize(predictorDb);
        detailedPredictor = JobBehaviorPredictor.newInstance(conf, DetailedPredictor.class);
        detailedPredictor.initialize(predictorDb);
        predictionTimeout = conf.getLong(POSUMConfiguration.PREDICTOR_TIMEOUT,
                POSUMConfiguration.PREDICTOR_TIMEOUT_DEFAULT);
    }

    void refresh() {
        long now = System.currentTimeMillis();
        if (fineGrained) {
            //TODO get metrics from all services and persist to database
            if (now - lastPrediction > predictionTimeout) {
                // make new predictions
                List<String> taskIds = dataStore.listIds(DataEntityDB.getMain(), DataEntityCollection.TASK, null);
                for (String taskId : taskIds) {
                    // prediction can throw exception if data model changes state during calculation
//                    try {
//                        Long duration = predictor.predictTaskDuration(taskId);
//                        dataStore.storeLogEntry(new LogEntry<>(LogEntry.Type.TASK_PREDICTION,
//                                TaskPrediction.newInstance(predictor.getClass().getSimpleName(), taskId, duration)));
//                    } catch (Exception e) {
//                        logger.debug("Could not predict task duration for " + taskId + " due to: ", e);
//                    }
                    Long duration;
                    try {
                        duration = basicPredictor.predictTaskDuration(taskId);
                        dataStore.storeLogEntry(new LogEntry<>(LogEntry.Type.TASK_PREDICTION,
                                TaskPredictionPayload.newInstance(basicPredictor.getClass().getSimpleName(), taskId, duration)));
                    } catch (Exception e) {
                        if (!(e instanceof POSUMException))
                            logger.error("Could not predict task duration for " + taskId + " due to: ", e);
                        else if (!e.getMessage().startsWith("Task has already finished"))
                            logger.debug("Could not predict task duration for " + taskId + " due to: ", e);
                    }
                    try {
                        duration = standardPredictor.predictTaskDuration(taskId);

                        dataStore.storeLogEntry(new LogEntry<>(LogEntry.Type.TASK_PREDICTION,
                                TaskPredictionPayload.newInstance(standardPredictor.getClass().getSimpleName(), taskId, duration)));
                    } catch (Exception e) {
                        if (!(e instanceof POSUMException))
                            logger.error("Could not predict task duration for " + taskId + " due to: ", e);
                        else if (!e.getMessage().startsWith("Task has already finished"))
                            logger.debug("Could not predict task duration for " + taskId + " due to: ", e);
                    }
                    try {
                        duration = detailedPredictor.predictTaskDuration(taskId);

                        dataStore.storeLogEntry(new LogEntry<>(LogEntry.Type.TASK_PREDICTION,
                                TaskPredictionPayload.newInstance(detailedPredictor.getClass().getSimpleName(), taskId, duration)));
                    } catch (Exception e) {
                        if (!(e instanceof POSUMException))
                            logger.error("Could not predict task duration for " + taskId + " due to: ", e);
                        else if (!e.getMessage().startsWith("Task has already finished"))
                            logger.debug("Could not predict task duration for " + taskId + " due to: ", e);
                    }
                }
                lastPrediction = now;
            }
        }

        // aggregate policy change decisions
        List<LogEntry<String>> policyChanges = dataStore.findLogs(LogEntry.Type.POLICY_CHANGE, lastCollectTime, now);
        if (policyChanges.size() > 0) {
            if (policyMap.getSchedulingStart() == 0)
                policyMap.setSchedulingStart(policyChanges.get(0).getTimestamp());
            for (LogEntry<String> change : policyChanges) {
                String policy = change.getDetails();
                PolicyMap.PolicyInfo info = policyMap.get(policy);
                if (!policy.equals(policyMap.getLastUsed())) {
                    if (policyMap.getLastUsed() != null) {
                        policyMap.get(policyMap.getLastUsed()).stop(change.getTimestamp());
                    }
                    policyMap.setLastUsed(policy);
                }
                info.start(change.getTimestamp());
            }
            dataStore.storeLogReport(new LogEntry<>(LogEntry.Type.POLICY_MAP, policyMap));
        }
        lastCollectTime = now;
    }

}

package org.apache.hadoop.tools.posum.database.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.field.TaskPrediction;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.PolicyMap;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.tools.posum.simulator.predictor.JobBehaviorPredictor;

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
    private JobBehaviorPredictor predictor;


    POSUMInfoCollector(Configuration conf, DataStore dataStore) {
        this.dataStore = dataStore;
        this.conf = conf;
        fineGrained = conf.getBoolean(POSUMConfiguration.FINE_GRAINED_MONITOR,
                POSUMConfiguration.FINE_GRAINED_MONITOR_DEFAULT);
        api = new POSUMAPIClient(conf);
        this.policyMap = new PolicyMap(conf);
        predictor = JobBehaviorPredictor.newInstance(conf);
        predictor.initialize(dataStore.bindTo(DataEntityDB.getMain()));
        predictionTimeout = conf.getLong(POSUMConfiguration.PREDICTOR_TIMEOUT,
                POSUMConfiguration.PREDICTOR_TIMEOUT_DEFAULT);
    }

    void collect() {
        long now = System.currentTimeMillis();
        if (fineGrained) {
            //TODO get metrics from all services and persist to database
            if (now - lastPrediction > predictionTimeout) {
                // make new predictions
                List<String> taskIds = dataStore.listIds(DataEntityDB.getMain(), DataEntityType.TASK, null);
                for (String taskId : taskIds) {
                    Long duration = predictor.predictTaskDuration(taskId);
                    dataStore.storeLogEntry(new LogEntry<>(LogEntry.Type.TASK_PREDICTION,
                            TaskPrediction.newInstance(taskId, duration)));
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

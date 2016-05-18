package org.apache.hadoop.tools.posum.database.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.PolicyMap;
import org.apache.hadoop.tools.posum.database.store.DataStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/4/16.
 */
public class POSUMInfoCollector {

    private static Log logger = LogFactory.getLog(POSUMInfoCollector.class);

    private final POSUMAPIClient api;
    private final DataEntityDB db = DataEntityDB.getLogs();
    private final DataStore dataStore;
    private final Configuration conf;
    private final boolean persistMetrics;
    private final PolicyMap policyMap;
    private long lastCollectTime = 0;


    POSUMInfoCollector(Configuration conf, DataStore dataStore) {
        this.dataStore = dataStore;
        this.conf = conf;
        persistMetrics = conf.getBoolean(POSUMConfiguration.MONITOR_PERSIST_METRICS,
                POSUMConfiguration.MONITOR_PERSIST_METRICS_DEFAULT);
        api = new POSUMAPIClient(conf);
        this.policyMap = new PolicyMap(conf);
    }

    void collect() {
        long now = System.currentTimeMillis();
        if (persistMetrics) {
            //TODO get metrics from all services and persist to database
        }
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
                    info.start(change.getTimestamp());
                }
            }
            dataStore.storeLogReport(new LogEntry<>(LogEntry.Type.POLICY_MAP, policyMap));
        }
        lastCollectTime = now;
    }

}

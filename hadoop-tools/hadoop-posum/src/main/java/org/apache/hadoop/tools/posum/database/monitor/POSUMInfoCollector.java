package org.apache.hadoop.tools.posum.database.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.tools.posum.database.store.DataStore;

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

    POSUMInfoCollector(Configuration conf, DataStore dataStore) {
        this.dataStore = dataStore;
        this.conf = conf;
        persistMetrics = conf.getBoolean(POSUMConfiguration.MONITOR_PERSIST_METRICS,
                POSUMConfiguration.MONITOR_PERSIST_METRICS_DEFAULT);
        api = new POSUMAPIClient(conf);
    }

    void collect() {
        if (persistMetrics) {
            //TODO get metrics from all services and persist to database
        }
    }

}

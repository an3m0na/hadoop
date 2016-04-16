package org.apache.hadoop.tools.posum.database.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.master.DataMasterContext;

import static java.lang.Thread.sleep;

/**
 * Created by ane on 2/4/16.
 */
public class HadoopMonitor extends AbstractService {

    private static Log logger = LogFactory.getLog(HadoopMonitor.class);

    private Configuration conf;
    private ClusterInfoCollector collector;
    private DataMasterContext context;
    private MonitorLoop loop;

    private class MonitorLoop extends Thread {
        private boolean exit = false;

        void exit() {
            exit = true;
            interrupt();
        }

        @Override
        public void run() {
            long time = conf.getLong(POSUMConfiguration.MONITOR_HEARTBEAT_MS,
                    POSUMConfiguration.MONITOR_HEARTBEAT_MS_DEFAULT);
            while (!exit) {
                try {
                    collector.collect();
                    sleep(time);
                } catch (InterruptedException e) {
                    logger.warn(e);
                }
            }
        }
    }

    public HadoopMonitor(DataMasterContext context) {
        super(HadoopMonitor.class.getName());
        this.context = context;
        this.loop = new MonitorLoop();
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        this.conf = conf;
        this.collector = new ClusterInfoCollector(conf, context.getDataStore());
    }

    @Override
    protected void serviceStart() throws Exception {
        super.serviceStart();
        loop.start();
    }

    @Override
    protected void serviceStop() throws Exception {
        loop.exit();
        super.serviceStop();
    }
}

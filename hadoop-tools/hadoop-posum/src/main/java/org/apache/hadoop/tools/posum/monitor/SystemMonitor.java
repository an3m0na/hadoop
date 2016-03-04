package org.apache.hadoop.tools.posum.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.DataStore;

/**
 * Created by ane on 2/4/16.
 */
public class SystemMonitor extends Thread implements Configurable {

    Configuration conf = new Configuration(false);
    private static Log logger = LogFactory.getLog(SystemMonitor.class);

    boolean exit = false;
    DatabaseFeeder feeder;

    public SystemMonitor(DataStore dataStore) {
        feeder = new DatabaseFeeder(dataStore);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public void exit() {
        exit = true;
        interrupt();
    }

    @Override
    public void run() {
        long time = conf.getLong(POSUMConfiguration.MONITOR_HEARTBEAT_MS,
                POSUMConfiguration.MONITOR_HEARTBEAT_MS_DEFAULT);
        while (!exit) {
            try {
                feeder.feedDatabase();
                sleep(time);
            } catch (InterruptedException e) {
                logger.warn(e);
            }
        }
    }


}

package org.apache.hadoop.tools.posum.database.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.monitor.ClusterInfoCollector;
import org.apache.hadoop.tools.posum.database.monitor.HadoopMonitor;
import org.apache.hadoop.tools.posum.database.monitor.POSUMInfoCollector;
import org.apache.hadoop.tools.posum.database.monitor.POSUMMonitor;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.tools.posum.web.DataMasterWebApp;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;

/**
 * Created by ane on 2/4/16.
 */
public class DataMaster extends CompositeService {
    private static final Log logger = LogFactory.getLog(DataMaster.class);

    private Dispatcher dispatcher;
    private DataMasterWebApp webApp;

    public DataMaster() {
        super(DataMaster.class.getName());
    }

    private DataMasterContext dmContext;
    private DataMasterCommService dmService;
    private DataStore dataStore;
    private HadoopMonitor hadoopMonitor;
    private POSUMMonitor posumMonitor;

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        dmContext = new DataMasterContext();

        dataStore = new DataStore(conf);
        dmContext.setDataStore(dataStore);
        dispatcher = new AsyncDispatcher();
        addIfService(dispatcher);

        dmContext.setDispatcher(dispatcher);

        //service to give database access to other POSUM processes
        dmService = new DataMasterCommService(dmContext);
        dmService.init(conf);
        addIfService(dmService);
        dmContext.setCommService(dmService);

        dmContext.setClusterInfo(new ClusterInfoCollector(conf, dataStore));
        hadoopMonitor = new HadoopMonitor(dmContext);
        hadoopMonitor.init(conf);
        addIfService(hadoopMonitor);

        dmContext.setPosumInfo(new POSUMInfoCollector(conf, dataStore));
        posumMonitor = new POSUMMonitor(dmContext);
        posumMonitor.init(conf);
        addIfService(posumMonitor);

        try {
            webApp = new DataMasterWebApp(dmContext,
                    conf.getInt(POSUMConfiguration.DM_WEBAPP_PORT,
                            POSUMConfiguration.DM_WEBAPP_PORT_DEFAULT));
        } catch (Exception e) {
            logger.error("Could not initialize web app", e);
        }

        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        super.serviceStart();
        if (webApp != null)
            webApp.start();
    }

    @Override
    protected void serviceStop() throws Exception {
        if (webApp != null)
            webApp.stop();
        super.serviceStop();
    }


    public static void main(String[] args) {
        try {
            Configuration conf = POSUMConfiguration.newInstance();
            DataMaster master = new DataMaster();
            master.init(conf);
            master.start();
        } catch (Exception e) {
            logger.fatal("Could not start Data Master", e);
        }
    }


}

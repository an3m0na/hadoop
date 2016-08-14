package org.apache.hadoop.tools.posum.database.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumMasterProcess;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.database.monitor.ClusterInfoCollector;
import org.apache.hadoop.tools.posum.database.monitor.HadoopMonitor;
import org.apache.hadoop.tools.posum.database.monitor.PosumInfoCollector;
import org.apache.hadoop.tools.posum.database.monitor.PosumMonitor;
import org.apache.hadoop.tools.posum.database.store.DataStoreImpl;
import org.apache.hadoop.tools.posum.database.store.LockBasedDataStore;
import org.apache.hadoop.tools.posum.web.DataMasterWebApp;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;

/**
 * Created by ane on 2/4/16.
 */
public class DataMaster extends CompositeService  implements PosumMasterProcess {
    private static final Log logger = LogFactory.getLog(DataMaster.class);

    private Dispatcher dispatcher;
    private DataMasterWebApp webApp;

    public DataMaster() {
        super(DataMaster.class.getName());
    }

    private DataMasterContext dmContext;
    private DataCommService commService;
    private LockBasedDataStore dataStore;
    private HadoopMonitor hadoopMonitor;
    private PosumMonitor posumMonitor;

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        dmContext = new DataMasterContext();

        dataStore = new DataStoreImpl(conf);
        dmContext.setDataBroker(Utils.exposeDataStoreAsBroker(dataStore));
        dispatcher = new AsyncDispatcher();
        addIfService(dispatcher);

        dmContext.setDispatcher(dispatcher);

        //service to give database access to other POSUM processes
        commService = new DataCommService(dmContext);
        commService.init(conf);
        addIfService(commService);
        dmContext.setCommService(commService);

        dmContext.setClusterInfo(new ClusterInfoCollector(conf, dataStore.bindTo(DataEntityDB.getMain())));
        hadoopMonitor = new HadoopMonitor(dmContext);
        hadoopMonitor.init(conf);
        addIfService(hadoopMonitor);

        dmContext.setPosumInfo(new PosumInfoCollector(conf, dmContext.getDataBroker()));
        posumMonitor = new PosumMonitor(dmContext);
        posumMonitor.init(conf);
        addIfService(posumMonitor);

        try {
            webApp = new DataMasterWebApp(dmContext,
                    conf.getInt(PosumConfiguration.DM_WEBAPP_PORT,
                            PosumConfiguration.DM_WEBAPP_PORT_DEFAULT));
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

    public String getConnectAddress() {
        if (commService != null)
            return commService.getConnectAddress();
        return null;
    }

    public static void main(String[] args) {
        try {
            Configuration conf = PosumConfiguration.newInstance();
            DataMaster master = new DataMaster();
            master.init(conf);
            master.start();
        } catch (Exception e) {
            logger.fatal("Could not start Data Master", e);
        }
    }


}

package org.apache.hadoop.tools.posum.data.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumMasterProcess;
import org.apache.hadoop.tools.posum.data.core.DataStoreImpl;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;
import org.apache.hadoop.tools.posum.data.monitor.ClusterInfoCollector;
import org.apache.hadoop.tools.posum.data.monitor.HadoopMonitor;
import org.apache.hadoop.tools.posum.data.monitor.PosumInfoCollector;
import org.apache.hadoop.tools.posum.data.monitor.PosumMonitor;
import org.apache.hadoop.tools.posum.web.DataMasterWebApp;

public class DataMaster extends CompositeService  implements PosumMasterProcess {
    private static final Log logger = LogFactory.getLog(DataMaster.class);

    private DataMasterWebApp webApp;

    public DataMaster() {
        super(DataMaster.class.getName());
    }

    private DataMasterContext dmContext;
    private DataMasterCommService commService;
    private LockBasedDataStore dataStore;
    private HadoopMonitor hadoopMonitor;
    private PosumMonitor posumMonitor;

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        dmContext = new DataMasterContext();

        dataStore = new DataStoreImpl(conf);
        dmContext.setDataStore(dataStore);

        //service to give database access to other POSUM processes
        commService = new DataMasterCommService(dmContext);
        commService.init(conf);
        addIfService(commService);
        dmContext.setCommService(commService);

        dmContext.setClusterInfo(new ClusterInfoCollector(conf, Database.extractFrom(dataStore, DatabaseReference.getMain())));
        hadoopMonitor = new HadoopMonitor(dmContext);
        hadoopMonitor.init(conf);
        addIfService(hadoopMonitor);

        dmContext.setPosumInfo(new PosumInfoCollector(conf, dmContext.getDataStore()));
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

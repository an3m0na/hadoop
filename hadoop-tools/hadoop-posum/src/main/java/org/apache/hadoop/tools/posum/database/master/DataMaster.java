package org.apache.hadoop.tools.posum.database.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.monitor.HadoopMonitor;
import org.apache.hadoop.tools.posum.database.client.DataStoreInterface;
import org.apache.hadoop.tools.posum.database.store.DataStoreImpl;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;

/**
 * Created by ane on 2/4/16.
 */
public class DataMaster extends CompositeService {
    private static final Log logger = LogFactory.getLog(DataMaster.class);

    private Dispatcher dispatcher;
    private String hostAddress;

    public DataMaster(String hostAddress) {
        super(DataMaster.class.getName());
        this.hostAddress = hostAddress;
    }

    private DataMasterContext dmContext;
    private DataMasterCommService dmService;
    private DataStoreInterface dataStoreInterface;
    private HadoopMonitor hadoopMonitor;

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        dmContext = new DataMasterContext();
        dmContext.setHostAddress(hostAddress);

        dataStoreInterface = new DataStoreImpl(conf);
        dmContext.setDataStoreInterface(dataStoreInterface);
        dispatcher = new AsyncDispatcher();
        addIfService(dispatcher);

        dmContext.setDispatcher(dispatcher);

        //service to give database access to other POSUM processes
        dmService = new DataMasterCommService(dmContext);
        dmService.init(conf);
        addIfService(dmService);

        hadoopMonitor = new HadoopMonitor(dmContext);
        hadoopMonitor.init(conf);
        addIfService(hadoopMonitor);

        super.serviceInit(conf);
    }

    public static void main(String[] args) {
        try {
            Configuration conf = POSUMConfiguration.newInstance();
            String hostAddress;
            if (args.length < 1) {
                logger.warn("No hostAddress supplied for master. Reverting to default");
                hostAddress = POSUMConfiguration.DM_ADDRESS_DEFAULT;
            } else {
                hostAddress = args[0];
            }
            DataMaster master = new DataMaster(hostAddress);
            master.init(conf);
            master.start();
        }catch (Exception e){
            logger.fatal("Could not start Data Master", e);
        }
    }


}

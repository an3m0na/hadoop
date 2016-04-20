package org.apache.hadoop.tools.posum.database.master;

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

    private Dispatcher dispatcher;

    public DataMaster() {
        super(DataMaster.class.getName());
    }

    private DataMasterContext dmContext;
    private DataMasterService dmService;
    private DataStoreInterface dataStoreInterface;
    private HadoopMonitor hadoopMonitor;

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        dataStoreInterface = new DataStoreImpl(conf);
        dmContext = new DataMasterContext();
        dmContext.setDataStoreInterface(dataStoreInterface);
        dispatcher = new AsyncDispatcher();
        addIfService(dispatcher);

        dmContext.setDispatcher(dispatcher);

        //service to give database access to other POSUM processes
        dmService = new DataMasterService(dmContext);
        dmService.init(conf);
        addIfService(dmService);

        hadoopMonitor = new HadoopMonitor(dmContext);
        hadoopMonitor.init(conf);
        addIfService(hadoopMonitor);

        super.serviceInit(conf);
    }

    @Override
    protected void serviceStop() throws Exception {
        if (dmService != null)
            dmService.stop();

        super.serviceStop();
    }

    public static void main(String[] args) {
        Configuration conf = POSUMConfiguration.newInstance();
        DataMaster master = new DataMaster();
        master.init(conf);
        master.start();
    }


}

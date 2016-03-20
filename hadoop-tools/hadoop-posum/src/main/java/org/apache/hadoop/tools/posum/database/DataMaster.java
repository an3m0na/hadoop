package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;

/**
 * Created by ane on 2/4/16.
 */
public class DataMaster extends CompositeService {

    Dispatcher dispatcher;

    public DataMaster() {
        super(DataMaster.class.getName());
    }

    protected DataMasterContext dmContext;
    private DataMasterService dmService;
    private DataStore dataStore;

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        dataStore = new DataStoreImpl(conf);
        dmContext.setDataStore(dataStore);
        dispatcher = new AsyncDispatcher();
        addIfService(dispatcher);

        dmContext.setDispatcher(dispatcher);

        //service to give database access to other POSUM processes
        dmService = new DataMasterService(dmContext);
        dmService.init(conf);

        super.serviceInit(conf);
    }
}

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

    protected DataMasterContext context;
    private DataMasterService dmService;

    @Override
    protected void serviceInit(Configuration conf) throws Exception {

        dispatcher = new AsyncDispatcher();
        addIfService(dispatcher);

        context.setDispatcher(dispatcher);

        //service to give database access to other POSUM processes
        dmService = new DataMasterService(context);
        dmService.init(conf);

        super.serviceInit(conf);
    }
}

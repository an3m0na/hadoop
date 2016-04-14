package org.apache.hadoop.tools.posum.core.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;

/**
 * Created by ane on 2/4/16.
 */
public class POSUMMaster extends CompositeService {

    private Dispatcher dispatcher;

    public POSUMMaster() {
        super(POSUMMaster.class.getName());
    }

    private POSUMMasterContext pmContext;
    private POSUMMasterService pmService;

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        pmContext = new POSUMMasterContext();
        dispatcher = new AsyncDispatcher();
        addIfService(dispatcher);

        pmContext.setDispatcher(dispatcher);

        //service to allow other processes to communicate with the master
        pmService = new POSUMMasterService(pmContext);
        pmService.init(conf);
        addIfService(pmService);

        super.serviceInit(conf);
    }

    @Override
    protected void serviceStop() throws Exception {
        if (pmService != null)
            pmService.stop();

        super.serviceStop();
    }

    public static void main(String[] args) {
        Configuration conf = POSUMConfiguration.newInstance();
        POSUMMaster master = new POSUMMaster();
        master.init(conf);
        master.start();
    }
}

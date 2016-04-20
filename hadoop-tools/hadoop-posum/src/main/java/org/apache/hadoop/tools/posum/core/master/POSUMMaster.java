package org.apache.hadoop.tools.posum.core.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.core.master.management.Orchestrator;
import org.apache.hadoop.tools.posum.core.master.management.POSUMEventType;
import org.apache.hadoop.tools.posum.core.scheduler.meta.client.MetaSchedulerClient;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;

/**
 * Created by ane on 2/4/16.
 */
public class POSUMMaster extends CompositeService {

    private Dispatcher dispatcher;
    private MetaSchedulerClient metaClient;

    public POSUMMaster() {
        super(POSUMMaster.class.getName());
    }

    private POSUMMasterContext pmContext;
    private MasterCommService commService;
    private Orchestrator orchestrator;

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        pmContext = new POSUMMasterContext();
        dispatcher = new AsyncDispatcher();
        addIfService(dispatcher);
        pmContext.setDispatcher(dispatcher);

        //service to communicate with other processes
        commService = new MasterCommService(pmContext);
        commService.init(conf);
        addIfService(commService);
        pmContext.setCommService(commService);

        // service that handles events and applies master logic
        orchestrator = new Orchestrator(pmContext);
        orchestrator.init(conf);
        addIfService(orchestrator);
        dispatcher.register(POSUMEventType.class, orchestrator);

        super.serviceInit(conf);
    }

    public static void main(String[] args) {
        Configuration conf = POSUMConfiguration.newInstance();
        POSUMMaster master = new POSUMMaster();
        master.init(conf);
        master.start();
    }
}

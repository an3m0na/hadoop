package org.apache.hadoop.tools.posum.core.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.core.master.management.Orchestrator;
import org.apache.hadoop.tools.posum.core.master.management.POSUMEventType;
import org.apache.hadoop.tools.posum.core.scheduler.meta.client.MetaSchedulerClient;
import org.apache.hadoop.tools.posum.web.MasterWebApp;
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
    private MasterWebApp webApp;

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

        webApp = new MasterWebApp(conf.getInt(POSUMConfiguration.MASTER_WEBAPP_PORT,
                POSUMConfiguration.MASTER_WEBAPP_PORT_DEFAULT));

        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        super.serviceStart();
        webApp.start();
    }

    @Override
    protected void serviceStop() throws Exception {
        webApp.stop();
        super.serviceStop();
    }

    public static void main(String[] args) {
        Configuration conf = POSUMConfiguration.newInstance();
        POSUMMaster master = new POSUMMaster();
        master.init(conf);
        master.start();
    }
}

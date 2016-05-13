package org.apache.hadoop.tools.posum.core.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.core.master.management.Orchestrator;
import org.apache.hadoop.tools.posum.core.master.management.POSUMEventType;
import org.apache.hadoop.tools.posum.core.scheduler.meta.client.MetaSchedulerClient;
import org.apache.hadoop.tools.posum.web.MasterWebApp;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;

import java.util.Arrays;

/**
 * Created by ane on 2/4/16.
 */
public class POSUMMaster extends CompositeService {
    private static Log logger = LogFactory.getLog(POSUMMaster.class);

    private Dispatcher dispatcher;
    private MetaSchedulerClient metaClient;
    private String hostAddress;


    public POSUMMaster(String hostAddress) {
        super(POSUMMaster.class.getName());
        this.hostAddress = hostAddress;
    }

    private POSUMMasterContext pmContext;
    private MasterCommService commService;
    private Orchestrator orchestrator;
    private MasterWebApp webApp;

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        pmContext = new POSUMMasterContext();
        pmContext.setHostAddress(hostAddress);
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

        try {
            webApp = new MasterWebApp(conf.getInt(POSUMConfiguration.MASTER_WEBAPP_PORT,
                    POSUMConfiguration.MASTER_WEBAPP_PORT_DEFAULT));
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
            String address;
            if (args.length < 1) {
                logger.warn("No hostAddress supplied for master. Reverting to default");
                address = POSUMConfiguration.PM_ADDRESS_DEFAULT;
            } else {
                address = args[0];
            }
            POSUMMaster master = new POSUMMaster(address);
            master.init(conf);
            master.start();
        } catch (Exception e) {
            logger.fatal("Could not start POSUM Master", e);
        }
    }
}

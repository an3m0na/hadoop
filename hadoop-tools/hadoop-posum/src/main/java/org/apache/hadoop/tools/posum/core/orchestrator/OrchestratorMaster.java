package org.apache.hadoop.tools.posum.core.orchestrator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumMasterProcess;
import org.apache.hadoop.tools.posum.core.orchestrator.management.Orchestrator;
import org.apache.hadoop.tools.posum.core.orchestrator.management.PosumEventType;
import org.apache.hadoop.tools.posum.web.OrchestratorWebApp;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;

/**
 * Created by ane on 2/4/16.
 */
public class OrchestratorMaster extends CompositeService implements PosumMasterProcess {
    private static Log logger = LogFactory.getLog(OrchestratorMaster.class);

    private Dispatcher dispatcher;

    public OrchestratorMaster() {
        super(OrchestratorMaster.class.getName());
    }

    private OrchestratorMasterContext pmContext;
    private OrchestratorCommService commService;
    private Orchestrator orchestrator;
    private OrchestratorWebApp webApp;

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        pmContext = new OrchestratorMasterContext();
        dispatcher = new AsyncDispatcher();
        addIfService(dispatcher);
        pmContext.setDispatcher(dispatcher);

        //service to communicate with other processes
        commService = new OrchestratorCommService(pmContext);
        commService.init(conf);
        addIfService(commService);
        pmContext.setCommService(commService);

        // service that handles events and applies master logic
        orchestrator = new Orchestrator(pmContext);
        orchestrator.init(conf);
        addIfService(orchestrator);
        dispatcher.register(PosumEventType.class, orchestrator);

        try {
            webApp = new OrchestratorWebApp(pmContext,
                    conf.getInt(PosumConfiguration.MASTER_WEBAPP_PORT,
                            PosumConfiguration.MASTER_WEBAPP_PORT_DEFAULT));
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
            OrchestratorMaster master = new OrchestratorMaster();
            master.init(conf);
            master.start();
        } catch (Exception e) {
            logger.fatal("Could not start POSUM Master", e);
        }
    }
}

package org.apache.hadoop.tools.posum.core.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.core.scheduler.meta.PolicyPortfolioService;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.net.InetSocketAddress;

/**
 * Created by ane on 3/19/16.
 */
public class POSUMMasterService extends CompositeService implements POSUMMasterProtocol {

    POSUMMasterContext pmContext;
    private Server server;
    private InetSocketAddress bindAddress;
    private PolicyPortfolioService portfolioService;

    public POSUMMasterService(POSUMMasterContext context) {
        super(POSUMMasterService.class.getName());
        this.pmContext = context;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        //service to interpret and respond to metascheduler updates
        portfolioService = new PolicyPortfolioService(pmContext);
        portfolioService.init(conf);
        addIfService(portfolioService);
    }

    @Override
    protected void serviceStart() throws Exception {
        YarnRPC rpc = YarnRPC.create(getConfig());
        InetSocketAddress masterServiceAddress = getConfig().getSocketAddr(
                POSUMConfiguration.PM_BIND_ADDRESS,
                POSUMConfiguration.PM_ADDRESS,
                POSUMConfiguration.DEFAULT_PM_ADDRESS,
                POSUMConfiguration.DEFAULT_PM_PORT);
        pmContext.setTokenSecretManager(new DummyTokenSecretManager());
        this.server =
                rpc.getServer(DataMasterProtocol.class, this, masterServiceAddress,
                        getConfig(), pmContext.getTokenSecretManager(),
                        getConfig().getInt(POSUMConfiguration.PM_SERVICE_THREAD_COUNT,
                                POSUMConfiguration.DEFAULT_PM_SERVICE_THREAD_COUNT));

        this.server.start();
        this.bindAddress = getConfig().updateConnectAddr(
                POSUMConfiguration.PM_BIND_ADDRESS,
                POSUMConfiguration.PM_ADDRESS,
                POSUMConfiguration.DEFAULT_PM_ADDRESS,
                server.getListenerAddress());
        super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
        if (this.server != null) {
            this.server.stop();
        }
        super.serviceStop();
    }

    @Override
    public SimpleResponse configureScheduler(ConfigurationRequest request) {
        return portfolioService.configureScheduler(request);
    }

    @Override
    public SimpleResponse initScheduler(ConfigurationRequest request) {
        return portfolioService.initScheduler(request);
    }

    @Override
    public SimpleResponse reinitScheduler(ConfigurationRequest request) {
        return portfolioService.reinitScheduler(request);
    }

    @Override
    public HandleEventResponse handleSchedulerEvent(HandleEventRequest request) {
        return portfolioService.handleSchedulerEvent(request);
    }

    @Override
    public SchedulerAllocateResponse allocateResources(SchedulerAllocateRequest request) {
        return portfolioService.allocateResources(request);
    }

    @Override
    public GetQueueInfoResponse getSchedulerQueueInfo(GetQueueInfoRequest request) {
        return portfolioService.getSchedulerQueueInfo(request);
    }
}

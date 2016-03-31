package org.apache.hadoop.tools.posum.core.master;

import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.net.InetSocketAddress;

/**
 * Created by ane on 3/19/16.
 */
public class POSUMMasterService extends AbstractService implements POSUMMasterProtocol {

    POSUMMasterContext pmContext;
    private Server server;
    private InetSocketAddress bindAddress;

    /**
     * Construct the service.
     *
     * @param context service pmContext
     */
    public POSUMMasterService(POSUMMasterContext context) {
        super(POSUMMasterService.class.getName());
        this.pmContext = context;
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
}

package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.DummyTokenSecretManager;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.net.InetSocketAddress;

/**
 * Created by ane on 3/19/16.
 */
public class DataMasterService extends AbstractService implements DataMasterProtocol {

    DataMasterContext context;
    private Server server;
    private InetSocketAddress bindAddress;

    /**
     * Construct the service.
     *
     * @param context service context
     */
    public DataMasterService(DataMasterContext context) {
        super(DataMasterService.class.getName());
        this.context = context;
    }

    @Override
    protected void serviceStart() throws Exception {
        YarnRPC rpc = YarnRPC.create(getConfig());
        InetSocketAddress masterServiceAddress = getConfig().getSocketAddr(
                null,
                POSUMConfiguration.DM_ADDRESS,
                POSUMConfiguration.DEFAULT_DM_ADDRESS,
                POSUMConfiguration.DEFAULT_DM_PORT);
        context.setTokenSecretManager(new DummyTokenSecretManager());
        this.server =
                rpc.getServer(ApplicationMasterProtocol.class, this, masterServiceAddress,
                        getConfig(), context.getTokenSecretManager(),
                        getConfig().getInt(POSUMConfiguration.DM_SERVICE_THREAD_COUNT,
                                POSUMConfiguration.DEFAULT_DM_SERVICE_THREAD_COUNT));

        this.server.start();
        this.bindAddress =
                getConfig().updateConnectAddr(null,
                        POSUMConfiguration.DM_ADDRESS,
                        POSUMConfiguration.DEFAULT_DM_ADDRESS,
                        server.getListenerAddress());
        super.serviceStart();
    }

}

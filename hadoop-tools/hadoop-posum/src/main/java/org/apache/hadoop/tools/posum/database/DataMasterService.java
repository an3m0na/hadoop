package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.records.profile.GeneralProfile;
import org.apache.hadoop.tools.posum.common.records.protocol.ListObjectsRequest;
import org.apache.hadoop.tools.posum.common.records.protocol.ListObjectsResponse;
import org.apache.hadoop.tools.posum.common.records.protocol.SingleObjectRequest;
import org.apache.hadoop.tools.posum.common.records.protocol.SingleObjectResponse;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.net.InetSocketAddress;

/**
 * Created by ane on 3/19/16.
 */
public class DataMasterService extends AbstractService implements DataMasterProtocol {

    DataMasterContext dmContext;
    private Server server;
    private InetSocketAddress bindAddress;

    /**
     * Construct the service.
     *
     * @param context service dmContext
     */
    public DataMasterService(DataMasterContext context) {
        super(DataMasterService.class.getName());
        this.dmContext = context;
    }

    @Override
    protected void serviceStart() throws Exception {
        YarnRPC rpc = YarnRPC.create(getConfig());
        InetSocketAddress masterServiceAddress = getConfig().getSocketAddr(
                null,
                POSUMConfiguration.DM_ADDRESS,
                POSUMConfiguration.DEFAULT_DM_ADDRESS,
                POSUMConfiguration.DEFAULT_DM_PORT);
        dmContext.setTokenSecretManager(new DummyTokenSecretManager());
        this.server =
                rpc.getServer(ApplicationMasterProtocol.class, this, masterServiceAddress,
                        getConfig(), dmContext.getTokenSecretManager(),
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

    @Override
    public ListObjectsResponse listObjects(ListObjectsRequest request) {
        return null;
    }

    @Override
    public SingleObjectResponse getObject(SingleObjectRequest request) {
        GeneralProfile ret = dmContext.getDataStore().findById(DataCollection.valueOf(request.getObjectClass()),
                request.getObjectId());
        return SingleObjectResponse.newInstance("Response for: " + ret.getId());
    }
}

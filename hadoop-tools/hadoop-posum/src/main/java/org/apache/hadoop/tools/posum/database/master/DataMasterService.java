package org.apache.hadoop.tools.posum.database.master;

import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.MultiEntityRequest;
import org.apache.hadoop.tools.posum.common.records.response.MultiEntityPayload;
import org.apache.hadoop.tools.posum.common.records.request.SingleEntityRequest;
import org.apache.hadoop.tools.posum.common.records.response.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

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
                POSUMConfiguration.DM_BIND_ADDRESS,
                POSUMConfiguration.DM_ADDRESS,
                POSUMConfiguration.DEFAULT_DM_ADDRESS,
                POSUMConfiguration.DEFAULT_DM_PORT);
        dmContext.setTokenSecretManager(new DummyTokenSecretManager());
        this.server =
                rpc.getServer(DataMasterProtocol.class, this, masterServiceAddress,
                        getConfig(), dmContext.getTokenSecretManager(),
                        getConfig().getInt(POSUMConfiguration.DM_SERVICE_THREAD_COUNT,
                                POSUMConfiguration.DEFAULT_DM_SERVICE_THREAD_COUNT));

        this.server.start();
        this.bindAddress = getConfig().updateConnectAddr(
                POSUMConfiguration.DM_BIND_ADDRESS,
                POSUMConfiguration.DM_ADDRESS,
                POSUMConfiguration.DEFAULT_DM_ADDRESS,
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
    public SimpleResponse<SingleEntityPayload> getEntity(SingleEntityRequest request) {
        try {
            GeneralDataEntity ret = dmContext.getDataStore().findById(request.getEntityType(), request.getId());
            SingleEntityPayload payload = SingleEntityPayload.newInstance(request.getEntityType(), ret);
            return SimpleResponse.newInstance(SimpleResponse.Type.SINGLE_ENTITY, payload);
        } catch (Exception e) {
            return SimpleResponse.newInstance(SimpleResponse.Type.SINGLE_ENTITY,
                    "Exception resolving request" + request, e);
        }
    }

    @Override
    public SimpleResponse<MultiEntityPayload> listEntities(MultiEntityRequest request) throws IOException, YarnException {
        try {
            List<GeneralDataEntity> ret = dmContext.getDataStore().find(request.getEntityType(), request.getProperties());
            MultiEntityPayload payload = MultiEntityPayload.newInstance(request.getEntityType(), ret);
            return SimpleResponse.newInstance(SimpleResponse.Type.MULTI_ENTITY, payload);
        } catch (Exception e) {
            return SimpleResponse.newInstance(SimpleResponse.Type.MULTI_ENTITY,
                    "Exception resolving request " + request, e);
        }
    }
}

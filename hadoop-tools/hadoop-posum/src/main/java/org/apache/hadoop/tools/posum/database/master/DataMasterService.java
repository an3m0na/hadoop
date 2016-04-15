package org.apache.hadoop.tools.posum.database.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.MultiEntityRequest;
import org.apache.hadoop.tools.posum.common.records.field.MultiEntityPayload;
import org.apache.hadoop.tools.posum.common.records.request.EntityByIdPayload;
import org.apache.hadoop.tools.posum.common.records.field.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by ane on 3/19/16.
 */
public class DataMasterService extends AbstractService implements DataMasterProtocol {

    private static Log logger = LogFactory.getLog(DataMasterService.class);

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
    public SimpleResponse<SingleEntityPayload> getEntity(SimpleRequest request) {

        try {
            switch (request.getType()) {
                case ENTITY_BY_ID:
                    EntityByIdPayload payload = (EntityByIdPayload) request.getPayload();
                    GeneralDataEntity ret = dmContext.getDataStoreInterface().findById(payload.getEntityType(), payload.getId());
                    SingleEntityPayload retPayload = SingleEntityPayload.newInstance(payload.getEntityType(), ret);
                    return SimpleResponse.newInstance(SimpleResponse.Type.SINGLE_ENTITY, retPayload);
                default:
                    return SimpleResponse.newInstance(SimpleResponse.Type.SINGLE_ENTITY,
                            "Could not recognize message type " + request.getType(), null);
            }
        } catch (Exception e) {
            return SimpleResponse.newInstance(SimpleResponse.Type.SINGLE_ENTITY, "Exception resolving request" + request, e);
        }
    }

    @Override
    public SimpleResponse<MultiEntityPayload> listEntities(MultiEntityRequest request) throws IOException, YarnException {
        try {
            List<GeneralDataEntity> ret = dmContext.getDataStoreInterface().find(request.getEntityType(), request.getProperties());
            MultiEntityPayload payload = MultiEntityPayload.newInstance(request.getEntityType(), ret);
            return SimpleResponse.newInstance(SimpleResponse.Type.MULTI_ENTITY, payload);
        } catch (Exception e) {
            return SimpleResponse.newInstance(SimpleResponse.Type.MULTI_ENTITY,
                    "Exception resolving request " + request, e);
        }
    }

    @Override
    public SimpleResponse handleSimpleRequest(SimpleRequest request) {
        try {
            switch (request.getType()) {
                case PING:
                    logger.info("Received ping with message: " + request.getPayload());
                    break;
                default:
                    return SimpleResponse.newInstance(false, "Could not recognize message type " + request.getType());
            }
        } catch (Exception e) {
            return SimpleResponse.newInstance("Exception when forwarding message type " + request.getType(), e);
        }
        return SimpleResponse.newInstance(true);
    }
}

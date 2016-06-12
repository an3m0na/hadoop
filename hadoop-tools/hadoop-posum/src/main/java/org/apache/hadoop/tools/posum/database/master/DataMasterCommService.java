package org.apache.hadoop.tools.posum.database.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.field.*;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.SearchRequest;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.core.master.client.POSUMMasterClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 3/19/16.
 */
public class DataMasterCommService extends CompositeService implements DataMasterProtocol {

    private static Log logger = LogFactory.getLog(DataMasterCommService.class);

    DataMasterContext dmContext;
    private Server dmServer;
    private InetSocketAddress bindAddress;
    private POSUMMasterClient masterClient;

    /**
     * Construct the service.
     *
     * @param context service dmContext
     */
    public DataMasterCommService(DataMasterContext context) {
        super(DataMasterCommService.class.getName());
        this.dmContext = context;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        masterClient = new POSUMMasterClient();
        masterClient.init(conf);
        addIfService(masterClient);

        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        YarnRPC rpc = YarnRPC.create(getConfig());
        InetSocketAddress masterServiceAddress = getConfig().getSocketAddr(
                POSUMConfiguration.DM_BIND_ADDRESS,
                POSUMConfiguration.DM_ADDRESS,
                POSUMConfiguration.DM_ADDRESS_DEFAULT,
                POSUMConfiguration.DM_PORT_DEFAULT);
        dmContext.setTokenSecretManager(new DummyTokenSecretManager());
        this.dmServer =
                rpc.getServer(DataMasterProtocol.class, this, masterServiceAddress,
                        getConfig(), dmContext.getTokenSecretManager(),
                        getConfig().getInt(POSUMConfiguration.DM_SERVICE_THREAD_COUNT,
                                POSUMConfiguration.DM_SERVICE_THREAD_COUNT_DEFAULT));

        this.dmServer.start();
        super.serviceStart();

        String connectAddress =
                NetUtils.getConnectAddress(this.dmServer.getListenerAddress()).toString();
        masterClient.register(Utils.POSUMProcess.DM,
                connectAddress.substring(connectAddress.indexOf("/") + 1));
    }

    @Override
    protected void serviceStop() throws Exception {
        if (this.dmServer != null) {
            this.dmServer.stop();
        }
        super.serviceStop();
    }

    @Override
    public SimpleResponse<SingleEntityPayload> getEntity(SimpleRequest request) {
        logger.debug("Got request for entity " + request.getType());
        try {
            switch (request.getType()) {
                case ENTITY_BY_ID:
                    EntityByIdPayload idPayload = (EntityByIdPayload) request.getPayload();
                    GeneralDataEntity ret =
                            dmContext.getDataStore().findById(idPayload.getEntityDB(),
                                    idPayload.getEntityType(),
                                    idPayload.getId());
                    SingleEntityPayload entityPayload = SingleEntityPayload.newInstance(idPayload.getEntityType(), ret);
                    return SimpleResponse.newInstance(SimpleResponse.Type.SINGLE_ENTITY, entityPayload);
                case JOB_FOR_APP:
                    JobForAppPayload jobForAppPayload = (JobForAppPayload) request.getPayload();
                    JobProfile jobProfile =
                            dmContext.getDataStore().getJobProfileForApp(jobForAppPayload.getEntityDB(),
                                    jobForAppPayload.getAppId(), jobForAppPayload.getUser());
                    entityPayload = SingleEntityPayload.newInstance(DataEntityType.JOB, jobProfile);
                    logger.debug("Returning profile " + jobProfile);
                    return SimpleResponse.newInstance(SimpleResponse.Type.SINGLE_ENTITY, entityPayload);
                default:
                    return SimpleResponse.newInstance(SimpleResponse.Type.SINGLE_ENTITY,
                            "Could not recognize message type " + request.getType(), null);
            }
        } catch (Exception e) {
            logger.error("Exception resolving request", e);
            return SimpleResponse.newInstance(SimpleResponse.Type.SINGLE_ENTITY,
                    "Exception resolving request " + request, e);
        }
    }

    @Override
    public SimpleResponse<MultiEntityPayload> listEntities(SearchRequest request)
            throws IOException, YarnException {
        try {
            List<GeneralDataEntity> ret =
                    dmContext.getDataStore().find(request.getEntityDB(),
                            request.getEntityType(),
                            request.getProperties(),
                            request.getOffset(),
                            request.getLimit());
            MultiEntityPayload payload = MultiEntityPayload.newInstance(request.getEntityType(), ret);
            return SimpleResponse.newInstance(SimpleResponse.Type.MULTI_ENTITY, payload);
        } catch (Exception e) {
            logger.error("Exception resolving request", e);
            return SimpleResponse.newInstance(SimpleResponse.Type.MULTI_ENTITY,
                    "Exception resolving request " + request, e);
        }
    }

    @Override
    public SimpleResponse<StringListPayload> listIds(SearchRequest request)
            throws IOException, YarnException {
        try {
            List<String> ret =
                    dmContext.getDataStore().listIds(request.getEntityDB(),
                            request.getEntityType(),
                            request.getProperties());
            StringListPayload payload = StringListPayload.newInstance(ret);
            return SimpleResponse.newInstance(SimpleResponse.Type.STRING_LIST, payload);
        } catch (Exception e) {
            logger.error("Exception resolving request", e);
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
                case LOG_POLICY_CHANGE:
                    dmContext.getDataStore().storeLogEntry(
                            new LogEntry<>(LogEntry.Type.POLICY_CHANGE, request.getPayload()));
                    break;
                case SAVE_FLEX_FIELDS:
                    SaveFlexFieldsPayload saveFlexFieldsPayload = (SaveFlexFieldsPayload) request.getPayload();
                    dmContext.getDataStore().saveFlexFields(
                            saveFlexFieldsPayload.getEntityDB(),
                            saveFlexFieldsPayload.getJobId(),
                            saveFlexFieldsPayload.getNewFields(),
                            saveFlexFieldsPayload.getForHistory()
                    );
                    break;
                default:
                    return SimpleResponse.newInstance(false, "Could not recognize message type " + request.getType());
            }
        } catch (Exception e) {
            logger.error("Exception resolving request", e);
            return SimpleResponse.newInstance("Exception when forwarding message type " + request.getType(), e);
        }
        return SimpleResponse.newInstance(true);
    }

    public Map<Utils.POSUMProcess, String> getSystemAddresses() {
        return masterClient.getSystemAddresses();
    }
}

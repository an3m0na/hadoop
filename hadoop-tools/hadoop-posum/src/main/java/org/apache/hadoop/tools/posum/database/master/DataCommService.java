package org.apache.hadoop.tools.posum.database.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.call.DatabaseCallType;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.records.payload.*;
import org.apache.hadoop.tools.posum.common.records.request.DatabaseCallExecutionRequest;
import org.apache.hadoop.tools.posum.common.records.request.SearchRequest;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.core.orchestrator.client.OrchestratorMasterClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 3/19/16.
 */
public class DataCommService extends CompositeService implements DataMasterProtocol {

    private static Log logger = LogFactory.getLog(DataCommService.class);

    DataMasterContext dmContext;
    private Server dmServer;
    private String connectAddress;
    private OrchestratorMasterClient masterClient;

    /**
     * Construct the service.
     *
     * @param context service dmContext
     */
    public DataCommService(DataMasterContext context) {
        super(DataCommService.class.getName());
        this.dmContext = context;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        masterClient = new OrchestratorMasterClient();
        masterClient.init(conf);
        addIfService(masterClient);

        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        YarnRPC rpc = YarnRPC.create(getConfig());
        InetSocketAddress masterServiceAddress = getConfig().getSocketAddr(
                PosumConfiguration.DM_BIND_ADDRESS,
                PosumConfiguration.DM_ADDRESS,
                PosumConfiguration.DM_ADDRESS_DEFAULT,
                PosumConfiguration.DM_PORT_DEFAULT);
        dmContext.setTokenSecretManager(new DummyTokenSecretManager());
        this.dmServer =
                rpc.getServer(DataMasterProtocol.class, this, masterServiceAddress,
                        getConfig(), dmContext.getTokenSecretManager(),
                        getConfig().getInt(PosumConfiguration.DM_SERVICE_THREAD_COUNT,
                                PosumConfiguration.DM_SERVICE_THREAD_COUNT_DEFAULT));

        this.dmServer.start();
        super.serviceStart();

        String fullAddress =
                NetUtils.getConnectAddress(this.dmServer.getListenerAddress()).toString();
        connectAddress = fullAddress.substring(fullAddress.indexOf("/") + 1);
        masterClient.register(Utils.PosumProcess.DM, connectAddress);
    }

    public String getConnectAddress() {
        return connectAddress;
    }

    @Override
    protected void serviceStop() throws Exception {
        if (this.dmServer != null) {
            this.dmServer.stop();
        }
        super.serviceStop();
    }

    @Override
    public SimpleResponse executeDatabaseCall(DatabaseCallExecutionRequest request) {
        DatabaseCallType callType = DatabaseCallType.fromMappedClass(request.getCall().getClass());
        logger.debug("Got request for call " + callType);
        if (callType == null) {
            String message = "Unrecognized call implementation " + request.getCall().getClass();
            logger.error(message);
            return SimpleResponse.newInstance(false, message);
        }
        try {
            return SimpleResponse.newInstance(callType.getPayloadType(),
                    request.getCall().executeCall(dmContext.getDataStore(), request.getEntityDB()));
        } catch (Exception e) {
            String message = "Exception executing call " + callType;
            logger.error(message, e);
            return SimpleResponse.newInstance(message, e);
        }
    }

    @Override
    public SimpleResponse<SingleEntityPayload> getEntity(SimpleRequest request) {
        logger.debug("Got request for entity " + request.getType());
        try {
            switch (request.getType()) {
                case ENTITY_BY_ID:
                    FindByIdCall idPayload = (FindByIdCall) request.getPayload();
                    GeneralDataEntity ret =
                            //TODO remove the main binding after transition to call based transactions is complete
                            dmContext.getDataStore().findById(DataEntityDB.getMain(),
                                    idPayload.getEntityCollection(),
                                    idPayload.getId());
                    SingleEntityPayload entityPayload = SingleEntityPayload.newInstance(idPayload.getEntityCollection(), ret);
                    return SimpleResponse.newInstance(PayloadType.SINGLE_ENTITY, entityPayload);
                case JOB_FOR_APP:
                    JobForAppPayload jobForAppPayload = (JobForAppPayload) request.getPayload();
                    JobProfile jobProfile;
                    if (jobForAppPayload.getEntityDB().equals(DataEntityDB.getMain()))
                        // use the cluster info to force fetch if app is new
                        jobProfile = dmContext.getClusterInfo().getCurrentProfileForApp(jobForAppPayload.getAppId(), jobForAppPayload.getUser());
                    else
                        jobProfile = dmContext.getDataStore().getJobProfileForApp(jobForAppPayload.getEntityDB(), jobForAppPayload.getAppId(), jobForAppPayload.getUser());
                    entityPayload = SingleEntityPayload.newInstance(DataEntityCollection.JOB, jobProfile);
                    logger.debug("Returning profile " + jobProfile);
                    return SimpleResponse.newInstance(PayloadType.SINGLE_ENTITY, entityPayload);
                default:
                    return SimpleResponse.newInstance(PayloadType.SINGLE_ENTITY,
                            "Could not recognize message type " + request.getType(), null);
            }
        } catch (Exception e) {
            logger.error("Exception resolving request", e);
            return SimpleResponse.newInstance(PayloadType.SINGLE_ENTITY,
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
                            request.getOffsetOrZero(),
                            request.getLimitOrZero());
            MultiEntityPayload payload = MultiEntityPayload.newInstance(request.getEntityType(), ret);
            return SimpleResponse.newInstance(PayloadType.MULTI_ENTITY, payload);
        } catch (Exception e) {
            logger.error("Exception resolving request", e);
            return SimpleResponse.newInstance(PayloadType.MULTI_ENTITY,
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
            return SimpleResponse.newInstance(PayloadType.STRING_LIST, payload);
        } catch (Exception e) {
            logger.error("Exception resolving request", e);
            return SimpleResponse.newInstance(PayloadType.MULTI_ENTITY,
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

    public Map<Utils.PosumProcess, String> getSystemAddresses() {
        return masterClient.getSystemAddresses();
    }
}

package org.apache.hadoop.tools.posum.data.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.client.orchestration.OrchestrationMasterClient;
import org.apache.hadoop.tools.posum.common.records.call.DatabaseCallType;
import org.apache.hadoop.tools.posum.common.records.payload.CollectionMapPayload;
import org.apache.hadoop.tools.posum.common.records.payload.DatabaseAlterationPayload;
import org.apache.hadoop.tools.posum.common.records.payload.DatabaseLockPayload;
import org.apache.hadoop.tools.posum.common.records.payload.PayloadType;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.request.DatabaseCallExecutionRequest;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.communication.CommUtils;
import org.apache.hadoop.tools.posum.common.util.communication.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.net.InetSocketAddress;
import java.util.Map;

public class DataMasterCommService extends CompositeService implements DataMasterProtocol {

  private static Log logger = LogFactory.getLog(DataMasterCommService.class);

  private DataMasterContext dmContext;
  private Server dmServer;
  private String connectAddress;
  private OrchestrationMasterClient masterClient;

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
    masterClient = new OrchestrationMasterClient();
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
    masterClient.register(CommUtils.PosumProcess.DM, connectAddress);
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
    logger.trace("Got request for call " + callType);
    if (callType == null) {
      String message = "Unrecognized call implementation " + request.getCall().getClass();
      logger.error(message);
      return SimpleResponse.newInstance(false, message);
    }
    try {
      return SimpleResponse.newInstance(callType.getPayloadType(),
        dmContext.getDataStore().execute(request.getCall(), request.getDatabase()));
    } catch (Exception e) {
      String message = "Exception executing call " + callType;
      logger.error(message, e);
      return SimpleResponse.newInstance(message, e);
    }
  }

  @Override
  public SimpleResponse handleSimpleRequest(SimpleRequest request) {
    try {
      switch (request.getType()) {
        case PING:
          logger.info("Received ping with message: " + request.getPayload());
          break;
        case LIST_COLLECTIONS:
          return SimpleResponse.newInstance(PayloadType.COLLECTION_MAP,
            CollectionMapPayload.newInstance(dmContext.getDataStore().listCollections()));
        case CLEAR_DATA:
          dmContext.getDataStore().clear();
          break;
        case CLEAR_DB:
          dmContext.getDataStore().clearDatabase(
            ((DatabaseAlterationPayload) request.getPayload()).getSourceDB());
          break;
        case COPY_DB:
          DatabaseAlterationPayload dbAlteration = (DatabaseAlterationPayload) request.getPayload();
          dmContext.getDataStore().copyDatabase(dbAlteration.getSourceDB(), dbAlteration.getDestinationDB());
          break;
        case COPY_COLL:
          dbAlteration = (DatabaseAlterationPayload) request.getPayload();
          dmContext.getDataStore().copyCollections(
            dbAlteration.getSourceDB(),
            dbAlteration.getDestinationDB(),
            dbAlteration.getTargetCollections()
            );
          break;
        case AWAIT_UPDATE:
          DatabaseLockPayload lock = (DatabaseLockPayload) request.getPayload();
          dmContext.getDataStore().awaitUpdate(lock.getDatabase(), lock.getMillis());
          break;
        case NOTIFY_UPDATE:
          lock = (DatabaseLockPayload) request.getPayload();
          dmContext.getDataStore().notifyUpdate(lock.getDatabase());
          break;
        case RESET:
          dmContext.getDataStore().clear();
          dmContext.getClusterInfoCollector().reset();
          dmContext.getPosumInfoCollector().reset();
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

  public Map<CommUtils.PosumProcess, String> getSystemAddresses() {
    return masterClient.getSystemAddresses();
  }
}

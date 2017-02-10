package org.apache.hadoop.tools.posum.simulation.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.client.data.DataMasterClient;
import org.apache.hadoop.tools.posum.client.orchestration.OrchestrationMasterClient;
import org.apache.hadoop.tools.posum.client.orchestration.Orchestrator;
import org.apache.hadoop.tools.posum.common.records.protocol.SimulatorMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.net.InetSocketAddress;

public class SimulationMasterCommService extends CompositeService implements SimulatorMasterProtocol {

  private static Log logger = LogFactory.getLog(SimulationMasterCommService.class);

  SimulationMasterContext context;
  private Server simulatorServer;
  private OrchestrationMasterClient masterClient;
  private DataMasterClient dataClient;
  private String connectAddress;

  SimulationMasterCommService(SimulationMasterContext context) {
    super(SimulationMasterCommService.class.getName());
    this.context = context;
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
      PosumConfiguration.SIMULATOR_BIND_ADDRESS,
      PosumConfiguration.SIMULATOR_ADDRESS,
      PosumConfiguration.SIMULATOR_ADDRESS_DEFAULT,
      PosumConfiguration.SIMULATOR_PORT_DEFAULT);
    this.simulatorServer =
      rpc.getServer(SimulatorMasterProtocol.class, this, masterServiceAddress,
        getConfig(), context.getTokenSecretManager(),
        getConfig().getInt(PosumConfiguration.SIMULATOR_SERVICE_THREAD_COUNT,
          PosumConfiguration.SIMULATOR_SERVICE_THREAD_COUNT_DEFAULT));

    this.simulatorServer.start();

    super.serviceStart();

    String fullAddress =
      NetUtils.getConnectAddress(this.simulatorServer.getListenerAddress()).toString();
    connectAddress = fullAddress.substring(fullAddress.indexOf("/") + 1);
    String dmAddress = masterClient.register(Utils.PosumProcess.SIMULATOR, connectAddress);
    dataClient = new DataMasterClient(dmAddress);
    dataClient.init(getConfig());
    addIfService(dataClient);
    dataClient.start();
  }

  public String getConnectAddress() {
    return connectAddress;
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.simulatorServer != null) {
      this.simulatorServer.stop();
    }
    super.serviceStop();
  }

  @Override
  public SimpleResponse handleSimpleRequest(SimpleRequest request) {
    try {
      switch (request.getType()) {
        case PING:
          logger.info("Received ping with message: " + request.getPayload());
          break;
        case START:
          context.getSimulator().startSimulation();
          break;
        default:
          return SimpleResponse.newInstance(false, "Could not recognize message type " + request.getType());
      }
    } catch (Exception e) {
      logger.error("Exception occurred while resolving request", e);
      return SimpleResponse.newInstance("Exception when forwarding message type " + request.getType(), e);
    }
    return SimpleResponse.newInstance(true);
  }

  public DataMasterClient getDataMaster() {
    return dataClient;
  }

  public Orchestrator getOrchestratorMaster() {
    return masterClient;
  }

}

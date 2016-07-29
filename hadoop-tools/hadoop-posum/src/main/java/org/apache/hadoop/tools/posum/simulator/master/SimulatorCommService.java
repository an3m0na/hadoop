package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.records.protocol.SimulatorProtocol;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.core.master.client.POSUMMasterClient;
import org.apache.hadoop.tools.posum.core.master.client.POSUMMasterInterface;
import org.apache.hadoop.tools.posum.database.client.DataMasterClient;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.net.InetSocketAddress;

/**
 * Created by ane on 3/19/16.
 */
class SimulatorCommService extends CompositeService implements SimulatorProtocol {

    private static Log logger = LogFactory.getLog(SimulatorCommService.class);

    SimulationMasterContext context;
    private Server simulatorServer;
    private POSUMMasterClient masterClient;
    private DataMasterClient dataClient;

    SimulatorCommService(SimulationMasterContext context) {
        super(SimulatorCommService.class.getName());
        this.context = context;
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
                POSUMConfiguration.SIMULATOR_BIND_ADDRESS,
                POSUMConfiguration.SIMULATOR_ADDRESS,
                POSUMConfiguration.SIMULATOR_ADDRESS_DEFAULT,
                POSUMConfiguration.SIMULATOR_PORT_DEFAULT);
        this.simulatorServer =
                rpc.getServer(SimulatorProtocol.class, this, masterServiceAddress,
                        getConfig(), context.getTokenSecretManager(),
                        getConfig().getInt(POSUMConfiguration.SIMULATOR_SERVICE_THREAD_COUNT,
                                POSUMConfiguration.SIMULATOR_SERVICE_THREAD_COUNT_DEFAULT));

        this.simulatorServer.start();

        super.serviceStart();

        String connectAddress =
                NetUtils.getConnectAddress(this.simulatorServer.getListenerAddress()).toString();
        String dmAddress = masterClient.register(Utils.POSUMProcess.SIMULATOR,
                connectAddress.substring(connectAddress.indexOf("/") + 1));
        dataClient = new DataMasterClient(dmAddress);
        dataClient.init(getConfig());
        addIfService(dataClient);
        dataClient.start();
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

    public DataMasterClient getDataClient() {
        return dataClient;
    }

    public POSUMMasterInterface getMaster() {
        return masterClient;
    }

}
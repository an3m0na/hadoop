package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.records.protocol.POSUMMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.SimulatorProtocol;
import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.core.master.client.POSUMMasterClient;
import org.apache.hadoop.tools.posum.core.master.client.POSUMMasterInterface;
import org.apache.hadoop.tools.posum.database.client.DataMasterClient;
import org.apache.hadoop.tools.posum.database.client.DataStoreInterface;
import org.apache.hadoop.tools.posum.simulator.master.client.SimulatorInterface;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.net.InetSocketAddress;

/**
 * Created by ane on 3/19/16.
 */
class SimulatorCommService extends CompositeService implements SimulatorProtocol {

    private static Log logger = LogFactory.getLog(SimulatorCommService.class);

    SimulatorInterface simulator;
    private Server simulatorServer;
    private InetSocketAddress bindAddress;
    private POSUMMasterClient masterClient;
    private DataMasterClient dataClient;
    private HandleSimResultRequest resultRequest;

    SimulatorCommService(SimulatorInterface simulator) {
        super(SimulatorCommService.class.getName());
        this.simulator = simulator;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        masterClient = new POSUMMasterClient();
        masterClient.init(conf);
        addIfService(masterClient);

        dataClient = new DataMasterClient();
        dataClient.init(conf);
        addIfService(dataClient);

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
                rpc.getServer(POSUMMasterProtocol.class, this, masterServiceAddress,
                        getConfig(), new DummyTokenSecretManager(),
                        getConfig().getInt(POSUMConfiguration.SIMULATOR_SERVICE_THREAD_COUNT,
                                POSUMConfiguration.SIMULATOR_SERVICE_THREAD_COUNT_DEFAULT));

        this.simulatorServer.start();
        this.bindAddress = getConfig().updateConnectAddr(
                POSUMConfiguration.SIMULATOR_BIND_ADDRESS,
                POSUMConfiguration.SIMULATOR_ADDRESS,
                POSUMConfiguration.SIMULATOR_ADDRESS_DEFAULT,
                simulatorServer.getListenerAddress());

        masterClient = new POSUMMasterClient();
        masterClient.init(getConfig());
        addIfService(masterClient);

        dataClient = new DataMasterClient();
        dataClient.init(getConfig());
        addIfService(dataClient);

        super.serviceStart();
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
                    simulator.startSimulation();
                    break;
                default:
                    return SimpleResponse.newInstance(false, "Could not recognize message type " + request.getType());
            }
        } catch (Exception e) {
            return SimpleResponse.newInstance("Exception when forwarding message type " + request.getType(), e);
        }
        return SimpleResponse.newInstance(true);
    }

    public DataStoreInterface getDataStore() {
        return dataClient;
    }

    public POSUMMasterInterface getMaster() {
        return masterClient;
    }

}

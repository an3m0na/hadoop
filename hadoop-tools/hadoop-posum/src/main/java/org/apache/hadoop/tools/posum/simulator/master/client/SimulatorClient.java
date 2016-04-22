package org.apache.hadoop.tools.posum.simulator.master.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.protocol.SimulatorProtocol;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.StandardClientProxyFactory;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * Created by ane on 4/13/16.
 */
public class SimulatorClient extends AbstractService implements SimulatorInterfaceClass {

    private static Log logger = LogFactory.getLog(SimulatorClient.class);

    private SimulatorProtocol simClient;
    private String connectAddress;

    public SimulatorClient(String connectAddress) {
        super(SimulatorClient.class.getName());
        this.connectAddress = connectAddress;
    }

    public String getConnectAddress() {
        return connectAddress;
    }

    @Override
    protected void serviceStart() throws Exception {
        final Configuration conf = getConfig();
        try {
            simClient = new StandardClientProxyFactory<>(conf,
                    connectAddress,
                    POSUMConfiguration.SIMULATOR_ADDRESS_DEFAULT,
                    POSUMConfiguration.SIMULATOR_PORT_DEFAULT,
                    SimulatorProtocol.class).createProxy();
            checkPing();
        } catch (IOException e) {
            throw new POSUMException("Could not init POSUMMaster client", e);
        }
        super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
        if (this.simClient != null) {
            RPC.stopProxy(this.simClient);
        }
        super.serviceStop();
    }

    private SimpleResponse sendSimpleRequest(SimpleRequest.Type type) {
        return sendSimpleRequest(type.name(), SimpleRequest.newInstance(type));
    }

    private SimpleResponse sendSimpleRequest(String kind, SimpleRequest request) {
        try {
            return Utils.handleError(kind, simClient.handleSimpleRequest(request));
        } catch (IOException | YarnException e) {
            throw new POSUMException("Error during RPC call", e);
        }
    }

    private void checkPing(){
        sendSimpleRequest("checkPing", SimpleRequest.newInstance(SimpleRequest.Type.PING, "Hello world!"));
        logger.info("Successfully connected to Simulator Master");
    }

    @Override
    public void startSimulation() {
        sendSimpleRequest(SimpleRequest.Type.START);
    }
}

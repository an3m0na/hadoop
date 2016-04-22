package org.apache.hadoop.tools.posum.core.master.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.protocol.POSUMMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.records.request.RegistrationRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.StandardClientProxyFactory;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * Created by ane on 4/13/16.
 */
public class POSUMMasterClient extends AbstractService implements POSUMMasterInterface {

    private static Log logger = LogFactory.getLog(POSUMMasterClient.class);

    public POSUMMasterClient() {
        super(POSUMMasterClient.class.getName());
    }

    private POSUMMasterProtocol pmClient;

    @Override
    protected void serviceStart() throws Exception {
        final Configuration conf = getConfig();
        try {
            pmClient = new StandardClientProxyFactory<>(conf,
                    conf.get(POSUMConfiguration.PM_ADDRESS),
                    POSUMConfiguration.PM_ADDRESS_DEFAULT,
                    POSUMConfiguration.PM_PORT_DEFAULT,
                    POSUMMasterProtocol.class).createProxy();
            checkPing();
        } catch (IOException e) {
            throw new POSUMException("Could not init POSUMMaster client", e);
        }
        super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
        if (this.pmClient != null) {
            RPC.stopProxy(this.pmClient);
        }
        super.serviceStop();
    }

    public SimpleResponse sendSimpleRequest(SimpleRequest.Type type) {
        return sendSimpleRequest(type.name(), SimpleRequest.newInstance(type));
    }

    private SimpleResponse sendSimpleRequest(String kind, SimpleRequest request) {
        try {
            return Utils.handleError(kind, pmClient.handleSimpleRequest(request));
        } catch (IOException | YarnException e) {
            throw new POSUMException("Error during RPC call", e);
        }
    }

    private void checkPing() {
        sendSimpleRequest("checkPing", SimpleRequest.newInstance(SimpleRequest.Type.PING, "Hello world!"));
        logger.info("Successfully connected to POSUMMaster");
    }

    @Override
    public String register(Utils.POSUMProcess process, String address) {
        try {
            return Utils.handleError("register",
                    pmClient.registerProcess(RegistrationRequest.newInstance(process, address))).getText();
        } catch (IOException | YarnException e) {
            throw new POSUMException("Error during RPC call", e);
        }
    }

    @Override
    public void handleSimulationResult(HandleSimResultRequest request) {
        try {
            Utils.handleError("handleSimulationResult", pmClient.handleSimulationResult(request));
        } catch (IOException | YarnException e) {
            throw new POSUMException("Error during RPC call", e);
        }
    }
}

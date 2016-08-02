package org.apache.hadoop.tools.posum.core.orchestrator.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.payload.StringStringMapPayload;
import org.apache.hadoop.tools.posum.common.records.protocol.OrchestratorMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.records.request.RegistrationRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.StandardClientProxyFactory;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ane on 4/13/16.
 */
public class OrchestratorMasterClient extends AbstractService implements OrchestratorMasterInterface {

    private static Log logger = LogFactory.getLog(OrchestratorMasterClient.class);

    public OrchestratorMasterClient() {
        super(OrchestratorMasterClient.class.getName());
    }

    private OrchestratorMasterProtocol pmClient;

    @Override
    protected void serviceStart() throws Exception {
        final Configuration conf = getConfig();
        try {
            pmClient = new StandardClientProxyFactory<>(conf,
                    conf.get(PosumConfiguration.PM_ADDRESS),
                    PosumConfiguration.PM_ADDRESS_DEFAULT,
                    PosumConfiguration.PM_PORT_DEFAULT,
                    OrchestratorMasterProtocol.class).createProxy();
            checkPing();
        } catch (IOException e) {
            throw new PosumException("Could not init OrchestratorMaster client", e);
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
            throw new PosumException("Error during RPC call", e);
        }
    }

    private void checkPing() {
        sendSimpleRequest("checkPing", SimpleRequest.newInstance(SimpleRequest.Type.PING, "Hello world!"));
        logger.info("Successfully connected to OrchestratorMaster");
    }

    @Override
    public String register(Utils.PosumProcess process, String address) {
        try {
            return Utils.handleError("register",
                    pmClient.registerProcess(RegistrationRequest.newInstance(process, address))).getText();
        } catch (IOException | YarnException e) {
            throw new PosumException("Error during RPC call", e);
        }
    }

    @Override
    public void handleSimulationResult(HandleSimResultRequest request) {
        try {
            Utils.handleError("handleSimulationResult", pmClient.handleSimulationResult(request));
        } catch (IOException | YarnException e) {
            throw new PosumException("Error during RPC call", e);
        }
    }

    public Map<Utils.PosumProcess, String> getSystemAddresses() {
        SimpleResponse response =
                sendSimpleRequest("getSystemAddresses", SimpleRequest.newInstance(SimpleRequest.Type.SYSTEM_ADDRESSES));
        StringStringMapPayload payload = (StringStringMapPayload) response.getPayload();
        if (payload != null) {
            Map<Utils.PosumProcess, String> ret = new HashMap<>();
            for (Map.Entry<String, String> entry : payload.getEntries().entrySet()) {
                ret.put(Utils.PosumProcess.valueOf(entry.getKey()), entry.getValue());
            }
            return ret;
        }
        return null;
    }
}

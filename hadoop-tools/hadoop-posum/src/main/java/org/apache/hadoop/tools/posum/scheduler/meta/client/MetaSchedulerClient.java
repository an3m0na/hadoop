package org.apache.hadoop.tools.posum.scheduler.meta.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.protocol.MetaSchedulerProtocol;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.StandardClientProxyFactory;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * Created by ane on 2/9/16.
 */
public class MetaSchedulerClient extends AbstractService implements MetaSchedulerInterface{

    private static Log logger = LogFactory.getLog(MetaSchedulerClient.class);

    private String connectAddress;

    public MetaSchedulerClient(String connectAddress) {
        super(MetaSchedulerClient.class.getName());
        this.connectAddress = connectAddress;
    }

    private MetaSchedulerProtocol metaClient;

    @Override
    protected void serviceStart() throws Exception {
        final Configuration conf = getConfig();
        try {
            metaClient = new StandardClientProxyFactory<>(conf,
                    connectAddress,
                    PosumConfiguration.SCHEDULER_ADDRESS_DEFAULT,
                    PosumConfiguration.SCHEDULER_PORT_DEFAULT,
                    MetaSchedulerProtocol.class).createProxy();
            checkPing();
        } catch (IOException e) {
            throw new PosumException("Could not init MetaScheduler client", e);
        }
        super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
        if (this.metaClient != null) {
            RPC.stopProxy(this.metaClient);
        }
        super.serviceStop();
    }

    private SimpleResponse sendSimpleRequest(SimpleRequest.Type type) {
        return sendSimpleRequest(type.name(), SimpleRequest.newInstance(type));
    }

    private SimpleResponse sendSimpleRequest(String kind, SimpleRequest request) {
        try {
            return Utils.handleError(kind, metaClient.handleSimpleRequest(request));
        } catch (IOException | YarnException e) {
            throw new PosumException("Error during RPC call", e);
        }
    }

    private void checkPing() {
        sendSimpleRequest("checkPing", SimpleRequest.newInstance(SimpleRequest.Type.PING, "Hello world!"));
        logger.info("Successfully connected to MetaScheduler");
    }

    @Override
    public void changeToPolicy(String policyName) {
        sendSimpleRequest("changeToPolicy", SimpleRequest.newInstance(SimpleRequest.Type.CHANGE_POLICY, policyName));
    }

    public String getConnectAddress(){
        return connectAddress;
    }
}

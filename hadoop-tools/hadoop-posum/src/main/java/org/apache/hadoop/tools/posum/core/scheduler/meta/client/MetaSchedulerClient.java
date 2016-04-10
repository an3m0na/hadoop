package org.apache.hadoop.tools.posum.core.scheduler.meta.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.records.protocol.MetaSchedulerProtocol;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.StandardClientProxyFactory;
import org.apache.hadoop.yarn.event.Event;

import java.io.IOException;

/**
 * Created by ane on 2/9/16.
 */
public class MetaSchedulerClient extends AbstractService {

    private static Log logger = LogFactory.getLog(MetaSchedulerClient.class);

    private Configuration posumConf;

    public MetaSchedulerClient() {
        super(MetaSchedulerClient.class.getName());
    }

    private MetaSchedulerProtocol metaClient;

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        this.posumConf = conf;
    }

    @Override
    protected void serviceStart() throws Exception {
        final Configuration conf = getConfig();
        try {
            metaClient = new StandardClientProxyFactory<>(conf, MetaSchedulerProtocol.class).createProxy();
        } catch (IOException e) {
            throw new POSUMException("Could not init POSUMMaster client", e);
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

    private void logIfError(SimpleResponse response, String message) {
        if (!response.getSuccessful()) {
            logger.error(message + "\n" + response.getText(), response.getException());
        }
    }

    public void handleRMEvent(Event event) {
        HandleRMEventRequest request = HandleRMEventRequest.newInstance(event);
        logIfError(metaClient.handleRMEvent(request), "Event handling unsuccessful");
    }
}

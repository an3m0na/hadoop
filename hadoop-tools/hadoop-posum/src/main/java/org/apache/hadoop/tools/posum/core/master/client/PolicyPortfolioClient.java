package org.apache.hadoop.tools.posum.core.master.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.protocol.ConfigurationRequest;
import org.apache.hadoop.tools.posum.common.records.protocol.HandleEventRequest;
import org.apache.hadoop.tools.posum.common.records.protocol.POSUMMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.StandardClientProxyFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by ane on 2/9/16.
 */
public class PolicyPortfolioClient extends AbstractService {

    private static Log logger = LogFactory.getLog(PolicyPortfolioClient.class);

    private Configuration posumConf;
    Set<String> relevantProps;

    public PolicyPortfolioClient() {
        super(PolicyPortfolioClient.class.getName());
    }

    private POSUMMasterProtocol pmClient;

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        this.posumConf = conf;
        relevantProps = new HashSet<>();
        String[] propStrings =
                posumConf.get(POSUMConfiguration.RELEVANT_SCHEDULER_CONFIGS,
                        POSUMConfiguration.DEFAULT_RELEVANT_SCHEDULER_CONFIGS).split(",");
        for (String propString : propStrings) {
            relevantProps.add(propString.trim());
        }
    }

    @Override
    protected void serviceStart() throws Exception {
        final Configuration conf = getConfig();
        try {
            pmClient = new StandardClientProxyFactory<>(conf, POSUMMasterProtocol.class).createProxy();
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

    private void logIfError(SimpleResponse response, String message) {
        if (!response.getSuccessful()) {
            logger.error(message + "\n" + response.getText() + "\n" + response.getDetails());
        }
    }

    private ConfigurationRequest composeConfRequest(Configuration conf){
        Map<String, String> properties = new HashMap<>();
        for (String prop : relevantProps) {
            properties.put(prop, conf.get(prop));
        }
        return ConfigurationRequest.newInstance(properties);
    }

    public void setConf(Configuration conf) {
        logIfError(pmClient.configureScheduler(composeConfRequest(conf)), "Configuration unsuccessful");
    }

    public void reinitScheduler(Configuration conf) {
        logIfError(pmClient.reinitScheduler(composeConfRequest(conf)), "Reinitialization unsuccessful");

    }

    public void initScheduler(Configuration conf) {
        logIfError(pmClient.initScheduler(composeConfRequest(conf)), "Initialization unsuccessful");
    }

    public void handleSchedulerEvent(SchedulerEvent event) {
        HandleEventRequest request = HandleEventRequest.newInstance(event);
        logIfError(pmClient.handleSchedulerEvent(request), "Event handling unsuccessful");
    }
}

package org.apache.hadoop.tools.posum.core.scheduler.meta;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.MetaSchedulerProtocol;
import org.apache.hadoop.tools.posum.common.records.reponse.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.core.master.client.POSUMMasterClient;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.net.InetSocketAddress;

/**
 * Created by ane on 4/13/16.
 */
public class MetaSchedulerCommService extends AbstractService implements MetaSchedulerProtocol {

    private static Log logger = LogFactory.getLog(MetaSchedulerCommService.class);

    private POSUMMasterClient masterClient;
    private Configuration posumConf;

    private Server portfolioServer;
    private InetSocketAddress bindAddress;


    public MetaSchedulerCommService() {
        super(MetaSchedulerCommService.class.getName());
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        posumConf = conf;
        masterClient = new POSUMMasterClient();
        masterClient.init(posumConf);
        masterClient.start();
        YarnRPC rpc = YarnRPC.create(getConfig());
        InetSocketAddress masterServiceAddress = getConfig().getSocketAddr(
                POSUMConfiguration.META_BIND_ADDRESS,
                POSUMConfiguration.META_ADDRESS,
                POSUMConfiguration.DEFAULT_META_ADDRESS,
                POSUMConfiguration.DEFAULT_META_PORT);
        this.portfolioServer =
                rpc.getServer(DataMasterProtocol.class, this, masterServiceAddress,
                        getConfig(), new DummyTokenSecretManager(),
                        getConfig().getInt(POSUMConfiguration.META_SERVICE_THREAD_COUNT,
                                POSUMConfiguration.DEFAULT_META_SERVICE_THREAD_COUNT));

        this.portfolioServer.start();
        this.bindAddress = getConfig().updateConnectAddr(
                POSUMConfiguration.META_BIND_ADDRESS,
                POSUMConfiguration.META_ADDRESS,
                POSUMConfiguration.DEFAULT_META_ADDRESS,
                portfolioServer.getListenerAddress());
    }

    @Override
    protected void serviceStop() throws Exception {
        if (this.portfolioServer != null) {
            this.portfolioServer.stop();
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
                default:
                    return SimpleResponse.newInstance(false, "Could not recognize message type " + request.getType());
            }
        } catch (Exception e) {
            return SimpleResponse.newInstance("Exception when forwarding message type " + request.getType(), e);
        }
        return SimpleResponse.newInstance(true);
    }
}

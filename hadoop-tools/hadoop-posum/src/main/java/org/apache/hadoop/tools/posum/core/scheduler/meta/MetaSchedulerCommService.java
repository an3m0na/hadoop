package org.apache.hadoop.tools.posum.core.scheduler.meta;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.records.protocol.MetaSchedulerProtocol;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.core.master.client.POSUMMasterClient;
import org.apache.hadoop.tools.posum.core.master.client.POSUMMasterInterface;
import org.apache.hadoop.tools.posum.core.scheduler.meta.client.MetaSchedulerInterface;
import org.apache.hadoop.tools.posum.database.client.DataMasterClient;
import org.apache.hadoop.tools.posum.database.client.DataStoreInterface;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.net.InetSocketAddress;

/**
 * Created by ane on 4/13/16.
 */
public class MetaSchedulerCommService extends CompositeService implements MetaSchedulerProtocol {

    private static Log logger = LogFactory.getLog(MetaSchedulerCommService.class);

    private POSUMMasterClient masterClient;
    private DataMasterClient dataClient;

    private Server metaServer;
    private MetaSchedulerInterface metaScheduler;
    private String bindAddress;

    MetaSchedulerCommService(MetaSchedulerInterface metaScheduler, String bindAddress) {
        super(MetaSchedulerCommService.class.getName());
        this.metaScheduler = metaScheduler;
        this.bindAddress = bindAddress;
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
                bindAddress,
                POSUMConfiguration.SCHEDULER_ADDRESS_DEFAULT,
                POSUMConfiguration.SCHEDULER_PORT_DEFAULT);
        this.metaServer =
                rpc.getServer(MetaSchedulerProtocol.class, this, masterServiceAddress,
                        getConfig(), new DummyTokenSecretManager(),
                        getConfig().getInt(POSUMConfiguration.SCHEDULER_SERVICE_THREAD_COUNT,
                                POSUMConfiguration.SCHEDULER_SERVICE_THREAD_COUNT_DEFAULT));

        this.metaServer.start();
        InetSocketAddress connectAddress = NetUtils.getConnectAddress(this.metaServer.getListenerAddress());

        super.serviceStart();

        String dmAddress = masterClient.register(Utils.POSUMProcess.SCHEDULER,
                connectAddress.getHostName() + ":" + connectAddress.getPort());
        dataClient = new DataMasterClient(dmAddress);
        dataClient.init(getConfig());
        addIfService(dataClient);
        dataClient.start();
    }

    @Override
    protected void serviceStop() throws Exception {
        if (this.metaServer != null) {
            this.metaServer.stop();
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
                case CHANGE_POLICY:
                    metaScheduler.changeToPolicy((String) request.getPayload());
                    break;
                default:
                    return SimpleResponse.newInstance(false, "Could not recognize message type " + request.getType());
            }
        } catch (Exception e) {
            logger.error(e);
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

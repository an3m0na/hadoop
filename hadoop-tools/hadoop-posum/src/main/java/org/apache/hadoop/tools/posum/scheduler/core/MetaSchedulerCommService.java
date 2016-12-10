package org.apache.hadoop.tools.posum.scheduler.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.protocol.MetaSchedulerProtocol;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.client.orchestration.OrchestrationMasterClient;
import org.apache.hadoop.tools.posum.client.orchestration.Orchestrator;
import org.apache.hadoop.tools.posum.client.scheduler.MetaScheduler;
import org.apache.hadoop.tools.posum.client.data.DataMasterClient;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.net.InetSocketAddress;

public class MetaSchedulerCommService extends CompositeService implements MetaSchedulerProtocol {

    private static Log logger = LogFactory.getLog(MetaSchedulerCommService.class);

    private OrchestrationMasterClient masterClient;
    private DataMasterClient dataClient;
    private Server metaServer;
    private MetaScheduler metaScheduler;
    private String bindAddress;

    MetaSchedulerCommService(MetaScheduler metaScheduler, String bindAddress) {
        super(MetaSchedulerCommService.class.getName());
        this.metaScheduler = metaScheduler;
        this.bindAddress = bindAddress;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        masterClient = new OrchestrationMasterClient();
        masterClient.init(conf);
        addIfService(masterClient);

        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        YarnRPC rpc = YarnRPC.create(getConfig());
        InetSocketAddress masterServiceAddress = getConfig().getSocketAddr(
                bindAddress,
                PosumConfiguration.SCHEDULER_ADDRESS,
                PosumConfiguration.SCHEDULER_ADDRESS_DEFAULT,
                PosumConfiguration.SCHEDULER_PORT_DEFAULT);
        this.metaServer =
                rpc.getServer(MetaSchedulerProtocol.class, this, masterServiceAddress,
                        getConfig(), new DummyTokenSecretManager(),
                        getConfig().getInt(PosumConfiguration.SCHEDULER_SERVICE_THREAD_COUNT,
                                PosumConfiguration.SCHEDULER_SERVICE_THREAD_COUNT_DEFAULT));

        this.metaServer.start();

        super.serviceStart();

        String connectAddress =
                NetUtils.getConnectAddress(this.metaServer.getListenerAddress()).toString();
        String dmAddress = masterClient.register(Utils.PosumProcess.SCHEDULER,
                connectAddress.substring(connectAddress.indexOf("/") + 1));
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
            logger.error("Exception handling simple request type " + request.getType(), e);
            return SimpleResponse.newInstance("Exception when forwarding message type " + request.getType(), e);
        }
        return SimpleResponse.newInstance(true);
    }

    public Database getDatabase() {
        if (dataClient == null)
            return null;
        return Database.extractFrom(dataClient, DatabaseReference.getMain());
    }

    public Orchestrator getMaster() {
        return masterClient;
    }

}

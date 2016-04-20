package org.apache.hadoop.tools.posum.core.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.records.request.RegistrationRequest;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.core.master.management.POSUMEvent;
import org.apache.hadoop.tools.posum.core.master.management.POSUMEventType;
import org.apache.hadoop.tools.posum.core.scheduler.meta.client.MetaSchedulerClient;
import org.apache.hadoop.tools.posum.core.scheduler.meta.client.MetaSchedulerInterface;
import org.apache.hadoop.tools.posum.database.client.DataMasterClient;
import org.apache.hadoop.tools.posum.database.client.DataStoreInterface;
import org.apache.hadoop.tools.posum.simulator.master.client.SimulatorClient;
import org.apache.hadoop.tools.posum.simulator.master.client.SimulatorInterface;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by ane on 3/19/16.
 */
public class MasterCommService extends CompositeService implements POSUMMasterProtocol {

    private static Log logger = LogFactory.getLog(MasterCommService.class);

    POSUMMasterContext pmContext;
    private Server server;
    private MetaSchedulerClient schedulerClient;
    private DataMasterClient dataClient;
    private SimulatorClient simulatorClient;

    public MasterCommService(POSUMMasterContext context) {
        super(MasterCommService.class.getName());
        this.pmContext = context;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        YarnRPC rpc = YarnRPC.create(getConfig());
        InetSocketAddress masterServiceAddress = getConfig().getSocketAddr(
                POSUMConfiguration.PM_BIND_ADDRESS,
                POSUMConfiguration.PM_ADDRESS,
                POSUMConfiguration.PM_ADDRESS_DEFAULT,
                POSUMConfiguration.PM_PORT_DEFAULT);
        pmContext.setTokenSecretManager(new DummyTokenSecretManager());
        this.server =
                rpc.getServer(POSUMMasterProtocol.class, this, masterServiceAddress,
                        getConfig(), pmContext.getTokenSecretManager(),
                        getConfig().getInt(POSUMConfiguration.PM_SERVICE_THREAD_COUNT,
                                POSUMConfiguration.PM_SERVICE_THREAD_COUNT_DEFAULT));

        this.server.start();

        super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
        if (this.server != null) {
            this.server.stop();
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

    @Override
    public SimpleResponse handleSimulationResult(HandleSimResultRequest resultRequest) throws IOException, YarnException {
        try {
            pmContext.getDispatcher().getEventHandler().handle(new POSUMEvent(POSUMEventType.SIMULATION_FINISH,
                    resultRequest.getResults()));
        } catch (Exception e) {
            return SimpleResponse.newInstance("Exception resolving request ", e);
        }
        return SimpleResponse.newInstance(true);
    }

    @Override
    public SimpleResponse registerProcess(RegistrationRequest request) throws IOException, YarnException {
        try {
            switch (request.getProcess()) {
                case PM:
                    throw new POSUMException("Now that's just absurd!");
                case DM:
                    dataClient = new DataMasterClient(request.getConnectAddress());
                    dataClient.init(getConfig());
                    addIfService(dataClient);
                    dataClient.start();
                    break;
                case SIMULATOR:
                    simulatorClient = new SimulatorClient(request.getConnectAddress());
                    simulatorClient.init(getConfig());
                    addIfService(simulatorClient);
                    simulatorClient.start();
                    break;
                case SCHEDULER:
                    schedulerClient = new MetaSchedulerClient(request.getConnectAddress());
                    schedulerClient.init(getConfig());
                    addIfService(schedulerClient);
                    simulatorClient.start();
                    break;
            }

        } catch (Exception e) {
            return SimpleResponse.newInstance("Exception resolving request ", e);
        }
        return SimpleResponse.newInstance(true, dataClient == null ? null : dataClient.getConnectAddress());
    }

    public MetaSchedulerInterface getScheduler() {
        return schedulerClient;
    }

    public DataStoreInterface getDataStore() {
        return dataClient;
    }

    public SimulatorInterface getSimulator() {
        return simulatorClient;
    }
}

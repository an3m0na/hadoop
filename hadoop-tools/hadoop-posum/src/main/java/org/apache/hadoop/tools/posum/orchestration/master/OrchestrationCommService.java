package org.apache.hadoop.tools.posum.orchestration.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.records.payload.PayloadType;
import org.apache.hadoop.tools.posum.common.records.payload.StringStringMapPayload;
import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.records.request.RegistrationRequest;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.orchestration.core.PosumEvent;
import org.apache.hadoop.tools.posum.orchestration.core.PosumEventType;
import org.apache.hadoop.tools.posum.client.scheduler.MetaSchedulerClient;
import org.apache.hadoop.tools.posum.client.scheduler.MetaScheduler;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.data.DataMasterClient;
import org.apache.hadoop.tools.posum.client.simulation.SimulatorClient;
import org.apache.hadoop.tools.posum.client.simulation.Simulator;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class OrchestrationCommService extends CompositeService implements OrchestratorMasterProtocol {

    private static Log logger = LogFactory.getLog(OrchestrationCommService.class);

    OrchestrationMasterContext pmContext;
    private Server server;
    private MetaSchedulerClient schedulerClient;
    private DataMasterClient dataClient;
    private SimulatorClient simulatorClient;

    private String connectAddress;

    public OrchestrationCommService(OrchestrationMasterContext context) {
        super(OrchestrationCommService.class.getName());
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
                PosumConfiguration.PM_BIND_ADDRESS,
                PosumConfiguration.PM_ADDRESS,
                PosumConfiguration.PM_ADDRESS_DEFAULT,
                PosumConfiguration.PM_PORT_DEFAULT);
        pmContext.setTokenSecretManager(new DummyTokenSecretManager());
        this.server =
                rpc.getServer(OrchestratorMasterProtocol.class, this, masterServiceAddress,
                        getConfig(), pmContext.getTokenSecretManager(),
                        getConfig().getInt(PosumConfiguration.PM_SERVICE_THREAD_COUNT,
                                PosumConfiguration.PM_SERVICE_THREAD_COUNT_DEFAULT));

        this.server.start();

        super.serviceStart();

        String fullAddress =
                NetUtils.getConnectAddress(this.server.getListenerAddress()).toString();
        connectAddress = fullAddress.substring(fullAddress.indexOf("/") + 1);
    }

    public String getConnectAddress() {
        return connectAddress;
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
                case SYSTEM_ADDRESSES:
                    Map<String, String> map = new HashMap<>(4);
                    map.put(Utils.PosumProcess.DM.name(), getDMAddress());
                    map.put(Utils.PosumProcess.SCHEDULER.name(), getPSAddress());
                    map.put(Utils.PosumProcess.SIMULATOR.name(), getSMAddress());
                    map.put(Utils.PosumProcess.OM.name(), connectAddress);
                    return SimpleResponse.newInstance(PayloadType.STRING_STRING_MAP,
                            StringStringMapPayload.newInstance(map));
                default:
                    return SimpleResponse.newInstance(false, "Could not recognize message type " + request.getType());
            }
        } catch (Exception e) {
            logger.error("Exception resolving request", e);
            return SimpleResponse.newInstance("Exception when forwarding message type " + request.getType(), e);
        }
        return SimpleResponse.newInstance(true);
    }

    @Override
    public SimpleResponse handleSimulationResult(HandleSimResultRequest resultRequest) throws IOException, YarnException {
        try {
            pmContext.getDispatcher().getEventHandler().handle(new PosumEvent(PosumEventType.SIMULATION_FINISH,
                    resultRequest.getResults()));
        } catch (Exception e) {
            logger.error("Exception resolving request", e);
            return SimpleResponse.newInstance("Exception resolving request ", e);
        }
        return SimpleResponse.newInstance(true);
    }

    private void checkDM() {
        if (dataClient == null || dataClient.getConnectAddress() == null)
            throw new PosumException("No DataMaster registered! Shutting down...");
    }

    @Override
    public SimpleResponse registerProcess(RegistrationRequest request) throws IOException, YarnException {
        try {
            logger.debug("Registration request type " + request.getProcess() + " on " + request.getConnectAddress());
            switch (request.getProcess()) {
                case DM:
                    dataClient = new DataMasterClient(request.getConnectAddress());
                    dataClient.init(getConfig());
                    addIfService(dataClient);
                    dataClient.start();
                    break;
                case SIMULATOR:
                    checkDM();
                    simulatorClient = new SimulatorClient(request.getConnectAddress());
                    simulatorClient.init(getConfig());
                    addIfService(simulatorClient);
                    simulatorClient.start();
                    pmContext.getDispatcher().getEventHandler().handle(new PosumEvent(PosumEventType.SIMULATOR_CONNECTED));
                    break;
                case SCHEDULER:
                    checkDM();
                    schedulerClient = new MetaSchedulerClient(request.getConnectAddress());
                    schedulerClient.init(getConfig());
                    addIfService(schedulerClient);
                    schedulerClient.start();
                    break;
                default:
                    throw new PosumException("Now that's just absurd!");
            }

        } catch (Exception e) {
            logger.error("Exception resolving request", e);
            return SimpleResponse.newInstance("Exception resolving request ", e);
        }
        return SimpleResponse.newInstance(true, dataClient.getConnectAddress());
    }

    public DataStore getDataBroker() {
        if (dataClient != null && dataClient.isInState(STATE.STARTED))
            return dataClient;
        else return null;
    }

    public MetaScheduler getScheduler() {
        if (schedulerClient != null && schedulerClient.isInState(STATE.STARTED))
            return schedulerClient;
        else return null;
    }

    public Simulator getSimulator() {
        if (simulatorClient != null && simulatorClient.isInState(STATE.STARTED))
            return simulatorClient;
        else return null;
    }

    public String getDMAddress() {
        if (dataClient != null && dataClient.isInState(STATE.STARTED)) {
            String address = dataClient.getConnectAddress();
            if (address != null) {
                return address.split(":")[0] + ":" + getConfig().getInt(PosumConfiguration.DM_WEBAPP_PORT,
                        PosumConfiguration.DM_WEBAPP_PORT_DEFAULT);
            }
        }
        return null;
    }

    public String getPSAddress() {
        if (schedulerClient != null && schedulerClient.isInState(STATE.STARTED)) {
            String address = schedulerClient.getConnectAddress();
            if (address != null) {
                return address.split(":")[0] + ":" + getConfig().getInt(PosumConfiguration.SCHEDULER_WEBAPP_PORT,
                        PosumConfiguration.SCHEDULER_WEBAPP_PORT_DEFAULT);
            }
        }
        return null;
    }

    public String getSMAddress() {
        if (simulatorClient != null && simulatorClient.isInState(STATE.STARTED)) {
            String address = simulatorClient.getConnectAddress();
            if (address != null) {
                return address.split(":")[0] + ":" + getConfig().getInt(PosumConfiguration.SIMULATOR_WEBAPP_PORT,
                        PosumConfiguration.SIMULATOR_WEBAPP_PORT_DEFAULT);
            }
        }
        return null;
    }
}

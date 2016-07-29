package org.apache.hadoop.tools.posum.core.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.field.StringStringMapPayload;
import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.records.request.RegistrationRequest;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.core.master.management.POSUMEvent;
import org.apache.hadoop.tools.posum.core.master.management.POSUMEventType;
import org.apache.hadoop.tools.posum.core.scheduler.meta.client.MetaSchedulerClient;
import org.apache.hadoop.tools.posum.core.scheduler.meta.client.MetaSchedulerInterface;
import org.apache.hadoop.tools.posum.database.client.DataMasterClient;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.tools.posum.database.master.DataMaster;
import org.apache.hadoop.tools.posum.simulator.master.client.SimulatorClient;
import org.apache.hadoop.tools.posum.simulator.master.client.SimulatorInterface;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

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

    private String connectAddress;

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

        connectAddress =
                NetUtils.getConnectAddress(this.server.getListenerAddress()).toString();
        connectAddress = connectAddress.substring(connectAddress.indexOf("/") + 1);
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
                    map.put(Utils.POSUMProcess.DM.name(), getDMAddress());
                    map.put(Utils.POSUMProcess.SCHEDULER.name(), getPSAddress());
                    map.put(Utils.POSUMProcess.SIMULATOR.name(), getSMAddress());
                    map.put(Utils.POSUMProcess.PM.name(), connectAddress);
                    return SimpleResponse.newInstance(SimpleResponse.Type.STRING_STRING_MAP,
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
            pmContext.getDispatcher().getEventHandler().handle(new POSUMEvent(POSUMEventType.SIMULATION_FINISH,
                    resultRequest.getResults()));
        } catch (Exception e) {
            logger.error("Exception resolving request", e);
            return SimpleResponse.newInstance("Exception resolving request ", e);
        }
        return SimpleResponse.newInstance(true);
    }

    private void checkDM() {
        if (dataClient == null || dataClient.getConnectAddress() == null)
            throw new POSUMException("No DataMaster registered! Shutting down...");
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
                    pmContext.getDispatcher().getEventHandler().handle(new POSUMEvent(POSUMEventType.SIMULATOR_CONNECTED));
                    break;
                case SCHEDULER:
                    checkDM();
                    schedulerClient = new MetaSchedulerClient(request.getConnectAddress());
                    schedulerClient.init(getConfig());
                    addIfService(schedulerClient);
                    schedulerClient.start();
                    break;
                default:
                    throw new POSUMException("Now that's just absurd!");
            }

        } catch (Exception e) {
            logger.error("Exception resolving request", e);
            return SimpleResponse.newInstance("Exception resolving request ", e);
        }
        return SimpleResponse.newInstance(true, dataClient.getConnectAddress());
    }

    public DBInterface getDB() {
        if (dataClient != null && dataClient.isInState(STATE.STARTED))
            return dataClient.bindTo(DataEntityDB.getMain());
        else return null;
    }

    public MetaSchedulerInterface getScheduler() {
        if (schedulerClient != null && schedulerClient.isInState(STATE.STARTED))
            return schedulerClient;
        else return null;
    }

    public SimulatorInterface getSimulator() {
        if (simulatorClient != null && simulatorClient.isInState(STATE.STARTED))
            return simulatorClient;
        else return null;
    }

    public String getDMAddress() {
        if (dataClient != null && dataClient.isInState(STATE.STARTED)) {
            String address = dataClient.getConnectAddress();
            if (address != null) {
                return address.split(":")[0] + ":" + getConfig().getInt(POSUMConfiguration.DM_WEBAPP_PORT,
                        POSUMConfiguration.DM_WEBAPP_PORT_DEFAULT);
            }
        }
        return null;
    }

    public String getPSAddress() {
        if (schedulerClient != null && schedulerClient.isInState(STATE.STARTED)) {
            String address = schedulerClient.getConnectAddress();
            if (address != null) {
                return address.split(":")[0] + ":" + getConfig().getInt(POSUMConfiguration.SCHEDULER_WEBAPP_PORT,
                        POSUMConfiguration.SCHEDULER_WEBAPP_PORT_DEFAULT);
            }
        }
        return null;
    }

    public String getSMAddress() {
        if (simulatorClient != null && simulatorClient.isInState(STATE.STARTED)) {
            String address = simulatorClient.getConnectAddress();
            if (address != null) {
                return address.split(":")[0] + ":" + getConfig().getInt(POSUMConfiguration.SIMULATOR_WEBAPP_PORT,
                        POSUMConfiguration.SIMULATOR_WEBAPP_PORT_DEFAULT);
            }
        }
        return null;
    }
}
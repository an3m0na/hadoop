package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.util.PolicyPortfolio;
import org.apache.hadoop.tools.posum.simulator.master.client.SimulatorInterface;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ane on 4/19/16.
 */
public class SimulatorImpl extends CompositeService implements SimulatorInterface {

    private static Log logger = LogFactory.getLog(SimulatorImpl.class);

    private SimulationMasterContext context;
    private PolicyPortfolio policies;
    private Map<String, Simulation> simulationMap;
    private HandleSimResultRequest resultRequest;

    public SimulatorImpl(SimulationMasterContext context) {
        super(SimulatorImpl.class.getName());
        this.context = context;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        policies = new PolicyPortfolio(conf);
        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        super.serviceStart();
    }

    @Override
    public synchronized void startSimulation() {
        resultRequest = HandleSimResultRequest.newInstance();
        simulationMap = new HashMap<>(policies.size());
        for (String policyName : policies.keySet()) {
            logger.trace("Starting simulation for " + policyName);
            Simulation simulation = new Simulation(this, policyName, context);
            simulationMap.put(policyName, simulation);
            simulation.start();
        }
    }

    synchronized void simulationDone(SimulationResultPayload result) {
        try {
            resultRequest.addResult(result);
            if (resultRequest.getResults().size() == policies.size()) {
                logger.trace("Sending simulation result request");
                context.getCommService().getMaster().handleSimulationResult(resultRequest);
            }
        } catch (Exception e) {
            logger.error("Error processing simulation", e);
        }
    }
}

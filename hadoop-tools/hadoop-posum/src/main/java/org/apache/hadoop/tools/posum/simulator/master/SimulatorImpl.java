package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.field.SimulationResult;
import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.PolicyMap;
import org.apache.hadoop.tools.posum.simulator.master.client.SimulatorInterface;
import org.apache.hadoop.tools.posum.simulator.predictor.BasicPredictor;
import org.apache.hadoop.tools.posum.simulator.predictor.JobBehaviorPredictor;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ane on 4/19/16.
 */
public class SimulatorImpl extends CompositeService implements SimulatorInterface {

    private static Log logger = LogFactory.getLog(SimulatorImpl.class);

    private JobBehaviorPredictor predictor;
    private SimulationMasterContext context;
    private PolicyMap policies;
    private Map<String, Simulation> simulationMap;
    private HandleSimResultRequest resultRequest;

    public SimulatorImpl(SimulationMasterContext context) {
        super(SimulatorImpl.class.getName());
        this.context = context;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {


        Class<? extends JobBehaviorPredictor> predictorClass = getConfig().getClass(
                POSUMConfiguration.PREDICTOR_CLASS,
                BasicPredictor.class,
                JobBehaviorPredictor.class
        );

        try {
            predictor = predictorClass.getConstructor(Configuration.class)
                    .newInstance(conf);
        } catch (Exception e) {
            throw new POSUMException("Could not instantiate predictor type " + predictorClass.getName());
        }
        policies = new PolicyMap(conf);

        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        super.serviceStart();
        //TODO rethink this when simulation db is up
        predictor.setDataStore(context.getCommService().getDataClient().bindTo(DataEntityDB.getSimulation()));
    }

    @Override
    public synchronized void startSimulation() {
        resultRequest = HandleSimResultRequest.newInstance();
        simulationMap = new HashMap<>(policies.size());
        for (String policyName : policies.keySet()) {
            logger.debug("Starting simulation for " + policyName);
            Simulation simulation = new Simulation(this, policyName, predictor);
            simulationMap.put(policyName, simulation);
            simulation.start();
        }
    }

    synchronized void simulationDone(SimulationResult result) {
        try {
            resultRequest.addResult(result);
            if (resultRequest.getResults().size() == policies.size()) {
                logger.debug("Sending simulation result request");
                context.getCommService().getMaster().handleSimulationResult(resultRequest);
            }
        } catch (Exception e) {
            logger.error("Error processing simulation", e);
        }
    }
}

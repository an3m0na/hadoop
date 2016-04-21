package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.records.field.SimulationResult;
import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.PolicyMap;
import org.apache.hadoop.tools.posum.database.client.DataStoreInterface;
import org.apache.hadoop.tools.posum.simulator.master.client.SimulatorInterface;
import org.apache.hadoop.tools.posum.simulator.predictor.BasicPredictor;
import org.apache.hadoop.tools.posum.simulator.predictor.JobBehaviorPredictor;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ane on 4/19/16.
 */
public class SimulatorImpl extends CompositeService implements SimulatorInterface {

    private JobBehaviorPredictor predictor;
    private SimulatorCommService commService;
    private PolicyMap policies;
    private Map<String, Simulation> simulationMap;
    private HandleSimResultRequest resultRequest;

    public SimulatorImpl() {
        super(SimulatorImpl.class.getName());
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        commService = new SimulatorCommService(this);
        commService.init(conf);
        addIfService(commService);

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
        predictor.setDataStore(commService.getDataStore());
    }

    @Override
    public void startSimulation() {
        resultRequest = HandleSimResultRequest.newInstance();
        simulationMap = new HashMap<>(policies.size());
        for (String policyName : policies.keySet()) {
            System.out.println("Starting simulation for " + policyName);
            Simulation simulation = new Simulation(this, policyName, predictor);
            simulationMap.put(policyName, simulation);
            simulation.start();
        }
    }

    void simulationDone(SimulationResult result) {
        System.out.println("Simulation done");
        resultRequest.addResult(result);
        if (resultRequest.getResults().size() == policies.size()) {
            System.out.println("Sending request");
            commService.getMaster().handleSimulationResult(resultRequest);
        }
    }
}

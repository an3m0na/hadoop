package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.PolicyMap;
import org.apache.hadoop.tools.posum.database.client.DataStoreInterface;
import org.apache.hadoop.tools.posum.simulator.master.client.SimulatorInterface;
import org.apache.hadoop.tools.posum.simulator.predictor.BasicPredictor;
import org.apache.hadoop.tools.posum.simulator.predictor.JobBehaviorPredictor;

import java.util.Map;

/**
 * Created by ane on 4/19/16.
 */
public class SimulatorImpl extends CompositeService implements SimulatorInterface {

    private JobBehaviorPredictor predictor;
    private SimulatorCommService commService;
    private PolicyMap policies;
    private Map<String, Simulation> simulationMap;

    public SimulatorImpl() {
        super(SimulatorImpl.class.getName());
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        commService = new SimulatorCommService(this);
        commService.init(conf);
        addIfService(commService);

        Class<? extends JobBehaviorPredictor> predictorClass = conf.getClass(
                POSUMConfiguration.PREDICTOR_CLASS,
                BasicPredictor.class,
                JobBehaviorPredictor.class
        );

        try {
            predictor = predictorClass.getConstructor(Configuration.class, DataStoreInterface.class)
                    .newInstance(conf, commService.getDataStore());
        } catch (Exception e) {
            throw new POSUMException("Could not instantiate predictor type " + predictorClass.getName());
        }
        policies = new PolicyMap(conf);
    }

    @Override
    public void startSimulation() {
        for (String policyName : policies.keySet()) {
            simulationMap.put(policyName, new Simulation(policyName, predictor));
        }
    }
}

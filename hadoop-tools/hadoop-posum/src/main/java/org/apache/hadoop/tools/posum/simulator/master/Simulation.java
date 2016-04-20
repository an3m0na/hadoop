package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.hadoop.tools.posum.common.records.field.CompoundScore;
import org.apache.hadoop.tools.posum.common.records.field.SimulationResult;
import org.apache.hadoop.tools.posum.simulator.predictor.JobBehaviorPredictor;

/**
 * Created by ane on 4/20/16.
 */
public class Simulation extends Thread {
    private volatile boolean exit = false;
    private SimulatorImpl simulator;
    private String policy;
    private JobBehaviorPredictor predictor;


    public Simulation(SimulatorImpl simulator, String policy, JobBehaviorPredictor predictor) {
        //TODO set up simulation
        this.simulator = simulator;
        this.policy = policy;
        this.predictor = predictor;
    }

    void exit() {
        exit = true;
        interrupt();
    }

    @Override
    public void run() {
        //TODO actual code
        try {
            sleep(3000);
            simulator.simulationDone(SimulationResult.newInstance(policy,
                    CompoundScore.newInstance(Math.random() * 10, null, null)));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
